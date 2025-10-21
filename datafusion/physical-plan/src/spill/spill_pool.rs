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

//! SpillPool: A reusable abstraction for managing spill files with FIFO semantics.
//!
//! # Overview
//!
//! The `SpillPool` provides a centralized mechanism for spilling record batches to disk
//! when memory is constrained. It manages a collection of spill files, each containing
//! multiple batches, with configurable maximum file sizes.
//!
//! # Design
//!
//! - **FIFO (Queue) semantics**: Batches are read in the order they were spilled
//! - **File handle reuse**: Multiple batches are written to the same file to minimize syscalls
//! - **Automatic file rotation**: When a file exceeds `max_file_size_bytes`, rotate to a new file
//! - **Sequential reading**: Uses IPC Stream format's natural sequential access pattern
//! - **Automatic cleanup**: Files are deleted once fully consumed
//!
//! # Usage Example
//!
//! ```ignore
//! let pool = SpillPool::new(
//!     100 * 1024 * 1024,  // 100MB max per file
//!     spill_manager,
//!     schema,
//! );
//!
//! // Spill batches - automatically rotates files when size limit reached
//! pool.push_batch(batch1)?;
//! pool.push_batch(batch2)?;
//! pool.flush()?;  // Finalize current file
//!
//! // Read back in FIFO order
//! let batch = pool.pop_batch()?.unwrap();  // Returns batch1
//! let batch = pool.pop_batch()?.unwrap();  // Returns batch2
//! ```

use std::collections::VecDeque;
use std::sync::Arc;
use std::task::Waker;

use parking_lot::Mutex;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::disk_manager::RefCountedTempFile;
use datafusion_execution::SendableRecordBatchStream;

use super::in_progress_spill_file::InProgressSpillFile;
use super::spill_manager::SpillManager;

/// A single spill file containing one or more record batches.
struct SpillFile {
    /// The temp file handle (auto-deletes when dropped)
    file: RefCountedTempFile,
}

impl SpillFile {
    fn new(file: RefCountedTempFile, _total_batches: usize, _total_size: usize) -> Self {
        Self { file }
    }
}

/// A pool of spill files that manages batch-level spilling with FIFO semantics.
///
/// Batches are written sequentially to files, with automatic rotation when the
/// configured size limit is reached. Reading is done via an infinite stream
/// that can read concurrently while writes continue.
///
/// # Thread Safety
///
/// `SpillPool` is not thread-safe and should be used from a single thread or
/// protected with appropriate synchronization (e.g., `Arc<Mutex<SpillPool>>`).
pub struct SpillPool {
    /// Maximum size in bytes before rotating to a new file
    max_file_size_bytes: usize,
    /// Queue of spill files (front = oldest, back = newest)
    files: VecDeque<SpillFile>,
    /// Current file being written to (if any)
    current_write_file: Option<InProgressSpillFile>,
    /// Size of current write file in bytes (estimated)
    current_write_size: usize,
    /// Number of batches written to current file
    current_batch_count: usize,
    /// SpillManager for creating files and tracking metrics
    spill_manager: Arc<SpillManager>,
    /// Schema for batches (kept for potential validation in debug builds)
    #[allow(dead_code)]
    schema: SchemaRef,
    /// Wakers to notify when new data is available for readers
    wakers: Vec<Waker>,
    /// Flag indicating no more writes will occur
    finalized: bool,
}

impl SpillPool {
    /// Creates a new SpillPool with FIFO semantics.
    ///
    /// # Arguments
    ///
    /// * `max_file_size_bytes` - Maximum size per file before rotation (e.g., 100MB)
    /// * `spill_manager` - Manager for file creation and metrics
    /// * `schema` - Schema for record batches
    pub fn new(
        max_file_size_bytes: usize,
        spill_manager: Arc<SpillManager>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            max_file_size_bytes,
            files: VecDeque::new(),
            current_write_file: None,
            current_write_size: 0,
            current_batch_count: 0,
            spill_manager,
            schema,
            wakers: Vec::new(),
            finalized: false,
        }
    }

    /// Marks the pool as finalized, indicating no more writes will occur.
    /// This allows readers to know when to stop waiting for more data.
    pub fn finalize(&mut self) {
        self.finalized = true;
        self.wake(); // Wake readers to check finalized status
    }

    /// Returns true if the pool has been finalized
    pub fn is_finalized(&self) -> bool {
        self.finalized
    }

    /// Creates an infinite stream reader for this pool.
    ///
    /// The stream automatically handles file rotation and can read concurrently
    /// while writes continue to the pool. When the stream catches up to the writer,
    /// it will return `Poll::Pending` and wait for more data.
    ///
    /// # Arguments
    ///
    /// * `pool` - Shared reference to the SpillPool
    /// * `spill_manager` - Manager for creating streams from spill files
    ///
    /// # Returns
    ///
    /// An infinite `SpillPoolStream` that never ends until dropped
    pub fn reader(
        pool: Arc<Mutex<Self>>,
        spill_manager: Arc<SpillManager>,
    ) -> SpillPoolStream {
        SpillPoolStream::new(pool, spill_manager)
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
        if batch.num_rows() == 0 {
            // Skip empty batches
            return Ok(());
        }

        let batch_size = batch.get_array_memory_size();

        // Check if we need to rotate to a new file
        let needs_rotation = if self.current_write_file.is_some() {
            // Rotate if adding this batch would exceed the max file size
            self.current_write_size + batch_size > self.max_file_size_bytes
        } else {
            // No current file, need to create one
            true
        };

        if needs_rotation && self.current_write_file.is_some() {
            // Finish current file and add to queue
            self.finish_current_file()?;
        }

        // Create new file if needed
        if self.current_write_file.is_none() {
            self.current_write_file =
                Some(self.spill_manager.create_in_progress_file("SpillPool")?);
            self.current_write_size = 0;
            self.current_batch_count = 0;
        }

        // Append batch to current file
        if let Some(ref mut file) = self.current_write_file {
            file.append_batch(batch)?;
        }

        self.current_write_size += batch_size;
        self.current_batch_count += 1;

        // Wake any waiting readers
        self.wake();

        Ok(())
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

    /// Finalizes the current write file and adds it to the files queue.
    ///
    /// This is called automatically when files reach the size limit, but can
    /// also be called explicitly to ensure all pending data is available for reading.
    pub fn flush(&mut self) -> Result<()> {
        if self.current_write_file.is_some() {
            self.finish_current_file()?;
        }
        Ok(())
    }

    // Private helper methods

    /// Finishes the current write file and moves it to the files queue.
    fn finish_current_file(&mut self) -> Result<()> {
        if let Some(mut file) = self.current_write_file.take() {
            // Finish writing to get the final file
            let finished_file = file.finish()?;

            if let Some(temp_file) = finished_file {
                // Get actual file size
                let actual_size = temp_file.current_disk_usage() as usize;

                // Create SpillFile and add to queue
                let spill_file =
                    SpillFile::new(temp_file, self.current_batch_count, actual_size);
                self.files.push_back(spill_file);
            }

            // Reset write state
            self.current_write_size = 0;
            self.current_batch_count = 0;

            // Wake any waiting readers since a new complete file is available
            self.wake();
        }

        Ok(())
    }
}

impl Drop for SpillPool {
    fn drop(&mut self) {
        // Flush any pending writes to ensure metrics are accurate
        // We ignore errors here since Drop doesn't allow returning errors
        let _ = self.flush();
    }
}

/// An infinite stream that reads from a SpillPool.
///
/// The stream automatically handles file rotation and reads from completed files.
/// When no completed files are available, it returns `Poll::Pending` and waits
/// for the writer to complete more files.
///
/// The stream never ends (`Poll::Ready(None)`) until it is dropped.
pub struct SpillPoolStream {
    /// Shared reference to the spill pool
    spill_pool: Arc<Mutex<SpillPool>>,
    /// SpillManager for creating streams from spill files
    spill_manager: Arc<SpillManager>,
    /// Current stream being read from
    current_stream: Option<SendableRecordBatchStream>,
    /// Schema for the batches
    schema: SchemaRef,
}

impl SpillPoolStream {
    /// Creates a new infinite stream from a SpillPool.
    ///
    /// # Arguments
    ///
    /// * `spill_pool` - Shared reference to the pool to read from
    /// * `spill_manager` - Manager for creating streams from spill files
    pub fn new(
        spill_pool: Arc<Mutex<SpillPool>>,
        spill_manager: Arc<SpillManager>,
    ) -> Self {
        let schema = {
            let pool = spill_pool.lock();
            Arc::clone(&pool.schema)
        };

        Self {
            spill_pool,
            spill_manager,
            current_stream: None,
            schema,
        }
    }
}

impl futures::Stream for SpillPoolStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use futures::StreamExt;
        use std::task::Poll;

        loop {
            // If we have a current stream, try to read from it
            if let Some(stream) = &mut self.current_stream {
                match stream.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok(batch))) => {
                        return Poll::Ready(Some(Ok(batch)));
                    }
                    Poll::Ready(Some(Err(e))) => {
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Ready(None) => {
                        // Current stream exhausted (finished reading a completed file)
                        self.current_stream = None;
                        // Continue loop to try getting next file
                    }
                    Poll::Pending => {
                        // Stream not ready yet
                        return Poll::Pending;
                    }
                }
            }

            // No current stream, try to get the next file to read
            // Only read from completed files in the queue
            let mut pool = self.spill_pool.lock();

            if let Some(spill_file) = pool.files.pop_front() {
                // We have a completed file to read
                let file = spill_file.file;
                drop(pool); // Release lock before creating stream

                match self.spill_manager.read_spill_as_stream(file, None) {
                    Ok(stream) => {
                        self.current_stream = Some(stream);
                        // Continue loop to poll the new stream
                    }
                    Err(e) => {
                        return Poll::Ready(Some(Err(e)));
                    }
                }
            } else {
                // No completed files available
                let is_finalized = pool.is_finalized();
                if is_finalized {
                    // Pool is finalized and no more files - we're done
                    return Poll::Ready(None);
                }
                // Register waker and wait for more files
                pool.register_waker(cx.waker().clone());
                return Poll::Pending;
            }
        }
    }
}

impl datafusion_execution::RecordBatchStream for SpillPoolStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    // TODO: Update tests to use the new stream-based API
    // The old tests tested pop_batch(), file_count(), batch_count(), is_empty()
    // which have been removed in favor of the SpillPoolStream interface.
    //
    // The SpillPool is now tested through integration tests in repartition/mod.rs
}
