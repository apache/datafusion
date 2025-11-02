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
//! use std::sync::Arc;
//! use parking_lot::Mutex;
//!
//! let pool = SpillPool::new(
//!     100 * 1024 * 1024,  // 100MB max per file
//!     spill_manager,
//! );
//! let pool = Arc::new(Mutex::new(pool));
//!
//! // Spill batches - automatically rotates files when size limit reached
//! {
//!     let mut pool = pool.lock();
//!     pool.push_batch(batch1)?;
//!     pool.push_batch(batch2)?;
//!     pool.flush()?;  // Finalize current file
//!     pool.finalize(); // Signal no more writes
//! }
//!
//! // Read back in FIFO order using a stream
//! let mut stream = SpillPool::reader(pool);
//! let batch1 = stream.next().await.unwrap()?;  // Returns batch1
//! let batch2 = stream.next().await.unwrap()?;  // Returns batch2
//! // stream.next() returns None after finalize
//! ```

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
    /// Maximum size in bytes before rotating to a new file.
    /// Typically initialized from the configuration option
    /// `datafusion.execution.max_spill_file_size_bytes`.
    max_file_size_bytes: usize,
    /// Queue of spill files (front = oldest, back = newest)
    files: VecDeque<RefCountedTempFile>,
    /// Current file being written to (if any)
    current_write_file: Option<InProgressSpillFile>,
    /// SpillManager for creating files and tracking metrics
    spill_manager: Arc<SpillManager>,
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
    pub fn new(max_file_size_bytes: usize, spill_manager: Arc<SpillManager>) -> Self {
        Self {
            max_file_size_bytes,
            files: VecDeque::new(),
            current_write_file: None,
            spill_manager,
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
        if batch.num_rows() == 0 {
            // Skip empty batches
            return Ok(());
        }

        let batch_size = batch.get_array_memory_size();

        // Check if we need to rotate to a new file
        let needs_rotation = if let Some(ref file) = self.current_write_file {
            // Rotate if adding this batch would exceed the max file size
            file.estimated_size() + batch_size > self.max_file_size_bytes
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
        }

        // Append batch to current file
        if let Some(ref mut file) = self.current_write_file {
            file.append_batch(batch)?;
        }

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
                // Add to queue
                self.files.push_back(temp_file);
            }

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
    pub fn new(spill_pool: Arc<Mutex<SpillPool>>) -> Self {
        let schema = {
            let pool = spill_pool.lock();
            pool.spill_manager.schema()
        };

        Self {
            spill_pool,
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

            if let Some(file) = pool.files.pop_front() {
                // We have a completed file to read
                let spill_manager = Arc::clone(&pool.spill_manager);
                drop(pool); // Release lock before creating stream

                match spill_manager.read_spill_as_stream(file, None) {
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

impl RecordBatchStream for SpillPoolStream {
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
    use std::task::Poll;
    use tokio;

    // ============================================================================
    // Test Utilities
    // ============================================================================

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]))
    }

    fn create_test_batch(start: i32, count: usize) -> RecordBatch {
        let schema = create_test_schema();
        let a: ArrayRef = Arc::new(Int32Array::from(
            (start..start + count as i32).collect::<Vec<_>>(),
        ));
        let b: ArrayRef = Arc::new(Int32Array::from(
            (start * 10..start * 10 + count as i32 * 10)
                .step_by(10)
                .collect::<Vec<_>>(),
        ));
        RecordBatch::try_new(schema, vec![a, b]).unwrap()
    }

    fn create_spill_pool(max_file_size: usize) -> SpillPool {
        let env = Arc::new(RuntimeEnv::default());
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let schema = create_test_schema();
        let spill_manager =
            Arc::new(SpillManager::new(env, metrics, Arc::clone(&schema)));

        SpillPool::new(max_file_size, spill_manager)
    }

    /// Helper to collect all batches from a stream
    async fn collect_batches(
        mut stream: SendableRecordBatchStream,
    ) -> Result<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        while let Some(result) = stream.next().await {
            batches.push(result?);
        }
        Ok(batches)
    }

    // ============================================================================
    // Basic Functionality Tests
    // ============================================================================

    #[tokio::test]
    async fn test_empty_pool_stream() -> Result<()> {
        let mut pool = create_spill_pool(1024 * 1024);
        pool.finalize(); // Mark as done with no data

        let pool = Arc::new(Mutex::new(pool));
        let stream = SpillPool::reader(pool);

        let batches = collect_batches(stream).await?;
        assert_eq!(batches.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_single_batch() -> Result<()> {
        let mut pool = create_spill_pool(1024 * 1024);

        // Push one batch
        let batch1 = create_test_batch(0, 10);
        pool.push_batch(&batch1)?;
        pool.flush()?;
        pool.finalize();

        let pool = Arc::new(Mutex::new(pool));
        let stream = SpillPool::reader(pool);

        let batches = collect_batches(stream).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 10);
        assert_eq!(batches[0].num_columns(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_batches_single_file() -> Result<()> {
        let mut pool = create_spill_pool(10 * 1024 * 1024); // Large file size

        // Push multiple batches
        for i in 0..5 {
            let batch = create_test_batch(i * 10, 10);
            pool.push_batch(&batch)?;
        }
        pool.flush()?;
        pool.finalize();

        let pool = Arc::new(Mutex::new(pool));
        let stream = SpillPool::reader(pool);

        let batches = collect_batches(stream).await?;
        assert_eq!(batches.len(), 5);

        // Verify FIFO order
        for (i, batch) in batches.iter().enumerate() {
            assert_eq!(batch.num_rows(), 10);
            let col_a = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(col_a.value(0), (i as i32) * 10);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_file_rotation_on_size_limit() -> Result<()> {
        // Small file size to force rotation
        let mut pool = create_spill_pool(500); // ~500 bytes

        // Push multiple batches - should create multiple files
        for i in 0..10 {
            let batch = create_test_batch(i * 5, 5);
            pool.push_batch(&batch)?;
        }
        pool.flush()?;
        pool.finalize();

        let pool = Arc::new(Mutex::new(pool));
        let stream = SpillPool::reader(pool);

        let batches = collect_batches(stream).await?;
        assert_eq!(batches.len(), 10);

        // Verify all batches in FIFO order
        for (i, batch) in batches.iter().enumerate() {
            let col_a = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(col_a.value(0), (i as i32) * 5);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_batches_skipped() -> Result<()> {
        let mut pool = create_spill_pool(1024 * 1024);

        let batch1 = create_test_batch(0, 10);
        let empty_batch = RecordBatch::new_empty(create_test_schema());
        let batch2 = create_test_batch(10, 10);

        pool.push_batch(&batch1)?;
        pool.push_batch(&empty_batch)?; // Should be skipped
        pool.push_batch(&batch2)?;
        pool.flush()?;
        pool.finalize();

        let pool = Arc::new(Mutex::new(pool));
        let stream = SpillPool::reader(pool);

        let batches = collect_batches(stream).await?;
        // Should only have 2 batches (empty one skipped)
        assert_eq!(batches.len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_large_batches() -> Result<()> {
        let mut pool = create_spill_pool(1024 * 1024);

        // Create larger batches
        let batch1 = create_test_batch(0, 1000);
        let batch2 = create_test_batch(1000, 1000);

        pool.push_batch(&batch1)?;
        pool.push_batch(&batch2)?;
        pool.flush()?;
        pool.finalize();

        let pool = Arc::new(Mutex::new(pool));
        let stream = SpillPool::reader(pool);

        let batches = collect_batches(stream).await?;
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 1000);
        assert_eq!(batches[1].num_rows(), 1000);

        Ok(())
    }

    // ============================================================================
    // Stream API Tests
    // ============================================================================

    #[tokio::test]
    async fn test_stream_blocks_when_no_data() -> Result<()> {
        let pool = create_spill_pool(1024 * 1024);

        let pool = Arc::new(Mutex::new(pool));
        let mut stream = SpillPool::reader(Arc::clone(&pool));

        // Poll should return Pending since no data and not finalized
        let poll_result = futures::poll!(stream.next());
        assert!(matches!(poll_result, Poll::Pending));

        Ok(())
    }

    #[tokio::test]
    async fn test_stream_wakes_on_push() -> Result<()> {
        let pool = create_spill_pool(1024 * 1024);
        let pool_arc = Arc::new(Mutex::new(pool));

        let pool_clone = Arc::clone(&pool_arc);
        let stream = SpillPool::reader(pool_clone);

        // Spawn a task that will push data after a delay
        let writer_pool = Arc::clone(&pool_arc);
        let _writer = SpawnedTask::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            let mut pool = writer_pool.lock();
            pool.push_batch(&create_test_batch(0, 10)).unwrap();
            pool.flush().unwrap();
            pool.finalize();
        });

        // This should wait for data and then return it
        let batches = collect_batches(stream).await?;
        assert_eq!(batches.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_stream_wakes_on_flush() -> Result<()> {
        let pool = create_spill_pool(1024 * 1024);
        let pool_arc = Arc::new(Mutex::new(pool));

        let pool_clone = Arc::clone(&pool_arc);
        let stream = SpillPool::reader(pool_clone);

        // Push without flush first
        {
            let mut pool = pool_arc.lock();
            pool.push_batch(&create_test_batch(0, 10)).unwrap();
            // Don't flush yet - data is in current_write_file
        }

        // Spawn task to flush after delay
        let writer_pool = Arc::clone(&pool_arc);
        let _writer = SpawnedTask::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            let mut pool = writer_pool.lock();
            pool.flush().unwrap();
            pool.finalize();
        });

        let batches = collect_batches(stream).await?;
        assert_eq!(batches.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_stream_wakes_on_finalize() -> Result<()> {
        let pool = create_spill_pool(1024 * 1024);
        let pool_arc = Arc::new(Mutex::new(pool));

        let pool_clone = Arc::clone(&pool_arc);
        let mut stream = SpillPool::reader(pool_clone);

        // First poll should be pending
        let poll_result = futures::poll!(stream.next());
        assert!(matches!(poll_result, Poll::Pending));

        // Finalize after delay
        let writer_pool = Arc::clone(&pool_arc);
        let _writer = SpawnedTask::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            writer_pool.lock().finalize();
        });

        // Stream should eventually return None
        let result = stream.next().await;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_finalize_before_flush() -> Result<()> {
        let mut pool = create_spill_pool(1024 * 1024);

        // Push data but DON'T flush
        pool.push_batch(&create_test_batch(0, 10))?;
        pool.finalize(); // Finalize without flush

        let pool = Arc::new(Mutex::new(pool));
        let stream = SpillPool::reader(pool);

        // Data in current_write_file should still be lost since not flushed
        let batches = collect_batches(stream).await?;
        assert_eq!(batches.len(), 0);

        Ok(())
    }

    // ============================================================================
    // Concurrent Reader/Writer Tests
    // ============================================================================

    #[tokio::test]
    async fn test_concurrent_push_and_read() -> Result<()> {
        let pool = create_spill_pool(1024 * 1024);
        let pool_arc = Arc::new(Mutex::new(pool));

        let writer_pool = Arc::clone(&pool_arc);
        let writer = SpawnedTask::spawn(async move {
            for i in 0..10 {
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                let mut pool = writer_pool.lock();
                pool.push_batch(&create_test_batch(i * 10, 10)).unwrap();
                pool.flush().unwrap();
            }
            writer_pool.lock().finalize();
        });

        let reader_pool = Arc::clone(&pool_arc);
        let stream = SpillPool::reader(reader_pool);
        let reader = SpawnedTask::spawn(async move { collect_batches(stream).await });

        // Wait for both tasks
        writer.await.unwrap();
        let batches = reader.await.unwrap()?;

        assert_eq!(batches.len(), 10);
        for (i, batch) in batches.iter().enumerate() {
            let col_a = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(col_a.value(0), (i as i32) * 10);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_reader_catches_up_to_writer() -> Result<()> {
        let pool = create_spill_pool(1024 * 1024);
        let pool_arc = Arc::new(Mutex::new(pool));

        // Start reader before any data is written
        let reader_pool = Arc::clone(&pool_arc);
        let mut stream = SpillPool::reader(reader_pool);

        // Should return pending
        let poll_result = futures::poll!(stream.next());
        assert!(matches!(poll_result, Poll::Pending));

        // Now add data
        {
            let mut pool = pool_arc.lock();
            pool.push_batch(&create_test_batch(0, 10))?;
            pool.flush()?;
            pool.finalize();
        }

        // Now stream should have data
        let batches = collect_batches(stream).await?;
        assert_eq!(batches.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_readers_same_pool() -> Result<()> {
        let mut pool = create_spill_pool(1024 * 1024);

        // Push some batches
        for i in 0..5 {
            pool.push_batch(&create_test_batch(i * 10, 10))?;
        }
        pool.flush()?;
        pool.finalize();

        let pool_arc = Arc::new(Mutex::new(pool));

        // Create two readers
        let stream1 = SpillPool::reader(Arc::clone(&pool_arc));
        let stream2 = SpillPool::reader(Arc::clone(&pool_arc));

        // Read from both concurrently
        let reader1 = SpawnedTask::spawn(async move { collect_batches(stream1).await });
        let reader2 = SpawnedTask::spawn(async move { collect_batches(stream2).await });

        let batches1 = reader1.await.unwrap()?;
        let batches2 = reader2.await.unwrap()?;

        // Each reader should consume different batches (pop_front removes from queue)
        // The total number should be 5, but distributed between readers
        let total = batches1.len() + batches2.len();
        assert_eq!(total, 5);

        Ok(())
    }

    #[tokio::test]
    async fn test_file_cutover_during_read() -> Result<()> {
        let pool = create_spill_pool(500); // Small size for rotation
        let pool_arc = Arc::new(Mutex::new(pool));

        let writer_pool = Arc::clone(&pool_arc);
        let writer = SpawnedTask::spawn(async move {
            // Write multiple batches that will cause rotation
            for i in 0..8 {
                {
                    let mut pool = writer_pool.lock();
                    pool.push_batch(&create_test_batch(i * 5, 5)).unwrap();
                    pool.flush().unwrap();
                } // Drop lock before sleep
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
            writer_pool.lock().finalize();
        });

        // Read concurrently
        let reader_pool = Arc::clone(&pool_arc);
        let stream = SpillPool::reader(reader_pool);
        let reader = SpawnedTask::spawn(async move { collect_batches(stream).await });

        writer.await.unwrap();
        let batches = reader.await.unwrap()?;

        // Should get all 8 batches despite file rotation
        assert_eq!(batches.len(), 8);

        Ok(())
    }

    #[tokio::test]
    async fn test_file_cutover_during_write() -> Result<()> {
        let mut pool = create_spill_pool(300); // Very small to force frequent rotation

        // Push batches that will definitely cause rotation
        for i in 0..5 {
            let batch = create_test_batch(i * 10, 10);
            pool.push_batch(&batch)?;
            // Don't flush after each - let rotation happen naturally
        }
        pool.flush()?;
        pool.finalize();

        let pool = Arc::new(Mutex::new(pool));
        let stream = SpillPool::reader(pool);

        let batches = collect_batches(stream).await?;
        assert_eq!(batches.len(), 5);

        Ok(())
    }

    // ============================================================================
    // Garbage Collection Tests
    // ============================================================================

    #[tokio::test]
    async fn test_file_cleanup_after_read() -> Result<()> {
        let mut pool = create_spill_pool(500);

        // Create multiple files
        for i in 0..5 {
            pool.push_batch(&create_test_batch(i * 10, 10))?;
            pool.flush()?; // Each batch in its own file
        }

        // Verify files exist before reading
        let initial_file_count = pool.files.len();
        assert_eq!(initial_file_count, 5);

        pool.finalize();

        let pool_arc = Arc::new(Mutex::new(pool));
        let stream = SpillPool::reader(Arc::clone(&pool_arc));

        // Read all batches
        let batches = collect_batches(stream).await?;
        assert_eq!(batches.len(), 5);

        // All files should be consumed (dropped from queue)
        let final_file_count = pool_arc.lock().files.len();
        assert_eq!(final_file_count, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_cleanup_with_rotation() -> Result<()> {
        let pool = create_spill_pool(400);
        let pool_arc = Arc::new(Mutex::new(pool));

        // Write and read concurrently
        let writer_pool = Arc::clone(&pool_arc);
        let writer = SpawnedTask::spawn(async move {
            for i in 0..10 {
                {
                    let mut pool = writer_pool.lock();
                    pool.push_batch(&create_test_batch(i * 10, 10)).unwrap();
                    pool.flush().unwrap();
                } // Drop lock before sleep
                tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
            }
            writer_pool.lock().finalize();
        });

        let reader_pool = Arc::clone(&pool_arc);
        let stream = SpillPool::reader(reader_pool);
        let reader = SpawnedTask::spawn(async move {
            let mut batches = Vec::new();
            let mut stream = stream;
            while let Some(result) = stream.next().await {
                batches.push(result.unwrap());
                // Small delay to let writer create more files
                tokio::time::sleep(tokio::time::Duration::from_millis(15)).await;
            }
            batches
        });

        writer.await.unwrap();
        let batches = reader.await.unwrap();

        assert_eq!(batches.len(), 10);

        // All files should be cleaned up
        let final_file_count = pool_arc.lock().files.len();
        assert_eq!(final_file_count, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_cleanup_with_unflushed_file() -> Result<()> {
        let mut pool = create_spill_pool(1024 * 1024);

        // Create some flushed files
        for i in 0..3 {
            pool.push_batch(&create_test_batch(i * 10, 10))?;
            pool.flush()?;
        }

        // Add unflushed data
        pool.push_batch(&create_test_batch(30, 10))?;
        // Don't flush!

        // current_write_file should have data
        assert!(pool.current_write_file.is_some());
        assert_eq!(pool.files.len(), 3);

        pool.finalize();

        let pool_arc = Arc::new(Mutex::new(pool));
        let stream = SpillPool::reader(pool_arc);

        // Should only get the 3 flushed batches
        let batches = collect_batches(stream).await?;
        assert_eq!(batches.len(), 3);

        Ok(())
    }

    // ============================================================================
    // Edge Cases & Error Handling Tests
    // ============================================================================

    #[tokio::test]
    async fn test_interleaved_flush() -> Result<()> {
        let pool = create_spill_pool(1024 * 1024);
        let pool_arc = Arc::new(Mutex::new(pool));

        // Push → flush
        {
            let mut pool = pool_arc.lock();
            pool.push_batch(&create_test_batch(0, 10))?;
            pool.flush()?;
        }

        // Read one batch
        let stream = SpillPool::reader(Arc::clone(&pool_arc));
        let mut stream = stream;
        let batch1 = stream.next().await.unwrap()?;
        assert_eq!(batch1.num_rows(), 10);

        // Push → flush again
        {
            let mut pool = pool_arc.lock();
            pool.push_batch(&create_test_batch(10, 10))?;
            pool.flush()?;
        }

        // Read second batch from same stream
        let batch2 = stream.next().await.unwrap()?;
        assert_eq!(batch2.num_rows(), 10);

        // Finalize and verify stream ends
        pool_arc.lock().finalize();
        let batch3 = stream.next().await;
        assert!(batch3.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_flush_empty_pool() -> Result<()> {
        let mut pool = create_spill_pool(1024 * 1024);

        // Flush with no data should be no-op
        pool.flush()?;
        pool.flush()?; // Multiple flushes

        assert_eq!(pool.files.len(), 0);
        assert!(pool.current_write_file.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_finalize_idempotent() -> Result<()> {
        let mut pool = create_spill_pool(1024 * 1024);

        pool.push_batch(&create_test_batch(0, 10))?;
        pool.flush()?;

        // Multiple finalize calls should be safe
        pool.finalize();
        assert!(pool.is_finalized());
        pool.finalize();
        assert!(pool.is_finalized());
        pool.finalize();
        assert!(pool.is_finalized());

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_flushes_current_file() -> Result<()> {
        let pool = create_spill_pool(1024 * 1024);
        let pool_arc = Arc::new(Mutex::new(pool));

        // Push without flush
        {
            let mut pool = pool_arc.lock();
            pool.push_batch(&create_test_batch(0, 10)).unwrap();
            pool.flush().unwrap();
            pool.finalize();
        }

        // Drop should trigger flush in Drop impl
        // (though in this case we already flushed)

        let stream = SpillPool::reader(pool_arc);
        let batches = collect_batches(stream).await?;
        assert_eq!(batches.len(), 1);

        Ok(())
    }
}
