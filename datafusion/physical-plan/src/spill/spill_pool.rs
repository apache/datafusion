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

//! Spill pool for managing spill files with FIFO semantics.

use std::collections::VecDeque;
use std::sync::Arc;
use std::task::Waker;
use futures::{Stream, StreamExt};

use parking_lot::Mutex;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::disk_manager::RefCountedTempFile;
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};

use super::in_progress_spill_file::InProgressSpillFile;
use super::spill_manager::SpillManager;

/// A pool of spill files that manages batch-level spilling with FIFO semantics.
///
/// # Overview
///
/// The `SpillPool` provides a centralized mechanism for spilling record batches to disk
/// when memory is constrained. It manages a collection of spill files, each containing
/// multiple batches, with configurable maximum file sizes.
///
/// # Design
///
/// - **FIFO (Queue) semantics**: Batches are read in the order they were spilled
/// - **File handle reuse**: Multiple batches are written to the same file to minimize syscalls
/// - **Automatic file rotation**: When a file exceeds `max_file_size_bytes`, rotate to a new file
/// - **Sequential reading**: Uses IPC Stream format's natural sequential access pattern
/// - **Automatic cleanup**: Files are deleted once fully consumed
/// - **Concurrent reading/writing**: Readers can poll and consume batches while writers continue
///   to spill more data, enabling pipelined processing
///
/// # Usage Example
///
/// This example demonstrates concurrent reading and writing. Note that the reader can start
/// polling for batches before the writer has finished, enabling pipelined processing:
///
/// ```
/// # use std::sync::Arc;
/// # use parking_lot::Mutex;
/// # use arrow::array::{ArrayRef, Int32Array};
/// # use arrow::datatypes::{DataType, Field, Schema};
/// # use arrow::record_batch::RecordBatch;
/// # use datafusion_common::Result;
/// # use datafusion_execution::runtime_env::RuntimeEnv;
/// # use datafusion_physical_plan::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
/// # use datafusion_physical_plan::SpillManager;
/// # use datafusion_physical_plan::spill::spill_pool::SpillPool;
/// # use datafusion_common_runtime::SpawnedTask;
/// # use futures::StreamExt;
/// #
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// # // Create test schema and batches
/// # let schema = Arc::new(Schema::new(vec![
/// #     Field::new("a", DataType::Int32, false),
/// # ]));
/// # let batch1 = RecordBatch::try_new(
/// #     Arc::clone(&schema),
/// #     vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
/// # )?;
/// # let batch2 = RecordBatch::try_new(
/// #     Arc::clone(&schema),
/// #     vec![Arc::new(Int32Array::from(vec![4, 5, 6])) as ArrayRef],
/// # )?;
/// # let batch3 = RecordBatch::try_new(
/// #     Arc::clone(&schema),
/// #     vec![Arc::new(Int32Array::from(vec![7, 8, 9])) as ArrayRef],
/// # )?;
/// #
/// # // Set up spill manager
/// # let env = Arc::new(RuntimeEnv::default());
/// # let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
/// # let spill_manager = Arc::new(SpillManager::new(env, metrics, schema));
/// #
/// // Create a spill pool
/// let pool = SpillPool::new(
///     100 * 1024 * 1024,  // 100MB max per file
///     spill_manager,
/// );
/// let pool = Arc::new(Mutex::new(pool));
///
/// // Create a reader that will consume batches as they become available
/// let stream = SpillPool::reader(pool.clone());
///
/// // Spawn a task to write batches concurrently
/// let writer = SpawnedTask::spawn({
///     let pool = pool.clone();
///     async move {
///         let mut pool = pool.lock();
///         pool.push_batch(&batch1).unwrap();
///         pool.push_batch(&batch2).unwrap();
///         pool.flush().unwrap();  // Finalize current file
///         pool.push_batch(&batch3).unwrap();
///         pool.flush().unwrap();  // Flush the last batch
///         pool.finalize(); // Signal no more writes
///     }
/// });
///
/// // Reader can start consuming immediately, even while writer is still working
/// let reader = SpawnedTask::spawn(async move {
///     let mut batches = vec![];
///     let mut stream = stream;
///     while let Some(result) = stream.next().await {
///         batches.push(result.unwrap());
///     }
///     batches
/// });
///
/// // Wait for both tasks to complete
/// writer.join().await.unwrap();
/// let batches = reader.join().await.unwrap();
///
/// assert_eq!(batches.len(), 3);
/// assert_eq!(batches[0].num_rows(), 3);
/// assert_eq!(batches[1].num_rows(), 3);
/// assert_eq!(batches[2].num_rows(), 3);
/// # Ok(())
/// # }
/// ```
///
/// # Thread Safety
///
/// `SpillPool` is not thread-safe and should be used from a single thread or
/// protected with appropriate synchronization (e.g., `Arc<Mutex<SpillPool>>`).
pub struct SpillPool {
    /// Maximum size in bytes before rotating to a new file.
    /// Typically initialized from the configuration option
    /// `datafusion.execution.max_spill_file_size_bytes`.
    max_file_size_bytes: usize,
    /// SpillManager for creating files and tracking metrics
    spill_manager: Arc<SpillManager>,
    /// Wakers to notify when new data is available for readers
    wakers: Vec<Waker>,
}

/// Shared state between writer and readers for an active spill file.
/// Protected by a Mutex to coordinate between concurrent readers and the writer.
struct ActiveSpillFileShared {
    /// Writer handle - taken (set to None) when finish() is called
    writer: Option<InProgressSpillFile>,
    /// Path to the spill file for creating readers
    file_path: RefCountedTempFile,
    /// Schema for creating readers
    schema: SchemaRef,
    /// Total number of batches written to this file
    batches_written: usize,
    /// Whether the writer has finished writing to this file
    writer_finished: bool,
}

/// Reader state for a SpillFile (owned by individual SpillFile instances).
/// This is kept separate from the shared state to avoid holding locks during I/O.
struct SpillFileReader {
    /// The actual stream reading from disk
    stream: SendableRecordBatchStream,
    /// Number of batches this reader has consumed
    batches_read: usize,
}

// impl SpillFile {
//     fn poll_next(&mut self) -> Option<Result<RecordBatch>> {
//         match self {
//             SpillFile::InProgress(active) => {
//                 // If there are no unread batches, we cannot read yet
//                 if active.unread_batches == 0 {
//                     return None;
//                 }
//             },
//             SpillFile::Completed(stream) => stream.next().await,
//         }
//     }
// }

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
        let (should_read, file_path, schema, batches_written) = {
            let shared = self.shared.lock();

            // Determine if we can read
            let batches_read = self.reader.as_ref().map_or(0, |r| r.batches_read);

            if batches_read < shared.batches_written {
                // More data available to read
                (true, shared.file_path.clone(), Arc::clone(&shared.schema), shared.batches_written)
            } else if shared.writer_finished {
                // No more data and writer is done - EOF
                return Poll::Ready(None);
            } else {
                // Caught up to writer, but writer still active - wait
                return Poll::Pending;
            }
        }; // Lock released here

        // Step 2: Lazy-create reader stream if needed
        if self.reader.is_none() && should_read {
            match self.spill_manager.read_spill_as_stream(file_path, None) {
                Ok(stream) => {
                    self.reader = Some(SpillFileReader {
                        stream,
                        batches_read: 0,
                    });
                }
                Err(e) => return Poll::Ready(Some(Err(e))),
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

impl SpillPool {
    /// Creates a new SpillPool with FIFO semantics.
    ///
    /// # Arguments
    ///
    /// * `max_file_size_bytes` - Maximum size per file before rotation (e.g., 100MB)
    /// * `spill_manager` - Manager for file creation and metrics
    pub fn new(max_file_size_bytes: usize, spill_manager: Arc<SpillManager>) -> Self {
        todo!()
    }

    /// Creates a stream reader for this pool.
    ///
    /// The stream automatically handles file rotation and can read concurrently
    /// while writes continue to the pool. When the stream catches up to the writer,
    /// it will return `Poll::Pending` and wait for more data.
    ///
    /// # Arguments
    ///
    /// * `pool` - Shared reference to the SpillPool
    ///
    /// # Returns
    ///
    /// A `SpillPoolStream` that returns batches in FIFO order and ends when the pool
    /// is finalized and all data has been read
    pub fn reader(pool: Arc<Mutex<Self>>) -> SendableRecordBatchStream {
        Box::pin(SpillPoolStream::new(pool))
    }

    /// Spills a batch to the pool, rotating files when necessary.
    ///
    /// If the current file would exceed `max_file_size_bytes` after adding
    /// this batch, the file is finalized and a new one is started.
    ///
    /// # Errors
    ///
    /// Returns an error if disk I/O fails or disk quota is exceeded.
    pub fn push_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        todo!()
    }

    /// Registers a waker to be notified when new data is available
    fn register_waker(&mut self, waker: Waker) {
        // Only register if not already present (avoid duplicates)
        if !self.wakers.iter().any(|w| w.will_wake(&waker)) {
            self.wakers.push(waker);
        }
    }

    /// Wakes all registered readers
    fn wake(&mut self) {
        for waker in self.wakers.drain(..) {
            waker.wake();
        }
    }
}

/// A stream that reads from a SpillPool in FIFO order.
///
/// The stream automatically handles file rotation and reads from completed files.
/// When no completed files are available, it returns `Poll::Pending` and waits
/// for the writer to complete more files.
///
/// The stream ends (`Poll::Ready(None)`) when the pool is finalized and all data has been read.
struct SpillPoolStream {
    /// Shared reference to the spill pool
    spill_pool: Arc<Mutex<SpillPool>>,
    /// Schema of the spilled data
    schema: SchemaRef,
    // Other state?
}

impl SpillPoolStream {
    /// Creates a new infinite stream from a SpillPool.
    ///
    /// # Arguments
    ///
    /// * `spill_pool` - Shared reference to the pool to read from
    pub fn new(spill_pool: Arc<Mutex<SpillPool>>) -> Self {
        todo!()
    }
}

impl Stream for SpillPoolStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        todo!();
    }
}

impl RecordBatchStream for SpillPoolStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
