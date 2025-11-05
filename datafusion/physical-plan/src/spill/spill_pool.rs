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
struct SpillPoolShared {
    /// Queue of ALL files (including the current write file if it exists).
    /// Readers always read from the front of this queue (FIFO).
    files: VecDeque<Arc<Mutex<ActiveSpillFileShared>>>,
    /// SpillManager for creating files and tracking metrics
    spill_manager: Arc<SpillManager>,
    /// Pool-level wakers to notify when new files are available
    wakers: Vec<Waker>,
}

impl SpillPoolShared {
    /// Creates a new shared pool state
    fn new(spill_manager: Arc<SpillManager>) -> Self {
        Self {
            files: VecDeque::new(),
            spill_manager,
            wakers: Vec::new(),
        }
    }

    /// Registers a waker to be notified when new data is available (pool-level)
    fn register_waker(&mut self, waker: Waker) {
        // Only register if not already present (avoid duplicates)
        if !self.wakers.iter().any(|w| w.will_wake(&waker)) {
            self.wakers.push(waker);
        }
    }

    /// Wakes all pool-level readers
    fn wake(&mut self) {
        for waker in self.wakers.drain(..) {
            waker.wake();
        }
    }
}

/// Writer for a spill pool. Provides exclusive write access with FIFO semantics.
pub struct SpillPoolWriter {
    /// Maximum size in bytes before rotating to a new file
    max_file_size_bytes: usize,
    /// Writer's reference to the current file (also in the shared files queue)
    current_write_file: Option<Arc<Mutex<ActiveSpillFileShared>>>,
    /// Shared state with readers
    shared: Arc<Mutex<SpillPoolShared>>,
}

impl SpillPoolWriter {
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

        // Create new file if we don't have one yet
        if self.current_write_file.is_none() {
            let spill_manager = {
                let shared = self.shared.lock();
                Arc::clone(&shared.spill_manager)
            };

            let writer = spill_manager.create_in_progress_file("SpillPool")?;
            // Clone the file so readers can access it immediately
            let file = writer.file().unwrap().clone();

            let file_shared = Arc::new(Mutex::new(ActiveSpillFileShared {
                writer: Some(writer),
                file: Some(file), // Set immediately so readers can access it
                batches_written: 0,
                estimated_size: 0,
                writer_finished: false,
                wakers: Vec::new(),
            }));

            // Push to shared queue and keep reference for writing
            {
                let mut shared = self.shared.lock();
                shared.files.push_back(Arc::clone(&file_shared));
                shared.wake(); // Wake readers waiting for new files
            }
            self.current_write_file = Some(file_shared);
        }

        let current_write_file = self.current_write_file.take();

        // Write batch to current file
        if let Some(current_file) = current_write_file {
            let mut file_shared = current_file.lock();

            // Append the batch
            if let Some(ref mut writer) = file_shared.writer {
                writer.append_batch(batch)?;
                file_shared.batches_written += 1;
                file_shared.estimated_size += batch_size;
            }

            // Wake readers waiting on this specific file
            file_shared.wake_all();

            // Check if we need to rotate
            let needs_rotation = file_shared.estimated_size > self.max_file_size_bytes;

            if needs_rotation {
                // Finish the IPC writer
                if let Some(mut writer) = file_shared.writer.take() {
                    writer.finish()?;
                }
                // Mark as finished so readers know not to wait for more data
                file_shared.writer_finished = true;
                // Wake readers waiting on this file (it's now finished)
                file_shared.wake_all();
            } else {
                // Release lock
                drop(file_shared);
                // Put back the current file for further writing
                self.current_write_file = Some(current_file);
            }
        }

        Ok(())
    }
}

impl Drop for SpillPoolWriter {
    fn drop(&mut self) {
        // Finalize the current file when writer is dropped
        if let Some(current_file) = self.current_write_file.take() {
            let mut file_shared = current_file.lock();

            // Finish the current writer if it exists
            if let Some(mut writer) = file_shared.writer.take() {
                // Ignore errors on drop - we're in destructor
                let _ = writer.finish();
            }

            // Mark as finished so readers know not to wait for more data
            file_shared.writer_finished = true;

            // Wake readers waiting on this file (it's now finished)
            file_shared.wake_all();
        }
    }
}

/// Creates a paired writer and reader for a spill pool with channel-like semantics.
///
/// This is the recommended way to create a spill pool. The writer has exclusive
/// write access, and the reader can consume batches in FIFO order. The reader
/// can start reading immediately while the writer continues to write more data.
///
/// # Arguments
///
/// * `max_file_size_bytes` - Maximum size per file before rotation
/// * `spill_manager` - Manager for file creation and metrics
///
/// # Returns
///
/// A tuple of `(SpillPoolWriter, SpillPoolReader)` that share the same underlying pool
///
/// # Example
///
/// ```ignore
/// let (mut writer, mut reader) = spill_pool::channel(1024 * 1024, spill_manager);
///
/// // Writer writes batches
/// writer.push_batch(&batch)?;
///
/// // Reader consumes batches (can happen concurrently)
/// while let Some(result) = reader.next().await {
///     let batch = result?;
///     // Process batch...
/// }
/// ```
pub fn channel(
    max_file_size_bytes: usize,
    spill_manager: Arc<SpillManager>,
) -> (SpillPoolWriter, SendableRecordBatchStream) {
    let schema = Arc::clone(spill_manager.schema());
    let shared = Arc::new(Mutex::new(SpillPoolShared::new(spill_manager)));

    let writer = SpillPoolWriter {
        max_file_size_bytes,
        current_write_file: None,
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
    /// Wakers for readers waiting on this specific file
    wakers: Vec<Waker>,
}

impl ActiveSpillFileShared {
    /// Registers a waker to be notified when new data is written to this file
    fn register_waker(&mut self, waker: Waker) {
        // Only register if not already present (avoid duplicates)
        if !self.wakers.iter().any(|w| w.will_wake(&waker)) {
            self.wakers.push(waker);
        }
    }

    /// Wakes all readers waiting on this file
    fn wake_all(&mut self) {
        for waker in self.wakers.drain(..) {
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
                match self.spill_manager.read_spill_as_stream(file, None) {
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
/// The stream automatically handles file rotation and reads from completed files.
/// When no completed files are available, it returns `Poll::Pending` and waits
/// for the writer to complete more files.
///
/// The stream will never end, it is an infinite stream.
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

            // Done with this file, no more files available
            // Register waker that will get notified when new files are added
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

    #[tokio::test]
    async fn test_basic_write_and_read() -> Result<()> {
        let (mut writer, mut reader) = create_spill_channel(1024 * 1024);

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
        let (mut writer, mut reader) = create_spill_channel(1024 * 1024);

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
        let (mut writer, mut reader) = create_spill_channel(1024 * 1024);

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
            assert_eq!(col.value(0), i * 10, "Batch {} not in FIFO order", i);
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
        let (mut writer, mut reader) = create_spill_channel(1024 * 1024);

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

        let (mut writer, mut reader) = create_spill_channel(batch_size);

        // Write first batch (should fit in first file)
        writer.push_batch(&batch1)?;

        // Write second batch (should trigger rotation to second file)
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

        // Read both batches
        let result1 = reader.next().await.unwrap()?;
        assert_eq!(result1.num_rows(), 10);

        let result2 = reader.next().await.unwrap()?;
        assert_eq!(result2.num_rows(), 10);

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_rotations() -> Result<()> {
        let batches = (0..10)
            .map(|i| create_test_batch(i * 10, 10))
            .collect::<Vec<_>>();

        let batch_size = batches[0].get_array_memory_size() * 2 + 1;

        // Very small max_file_size to force frequent rotations
        let (mut writer, mut reader) = create_spill_channel(batch_size);

        // Write many batches to cause multiple rotations
        for i in 0..10 {
            let batch = create_test_batch(i * 10, 10);
            writer.push_batch(&batch)?;
        }

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
                "Batch {} not in correct order after rotations",
                i
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_single_batch_larger_than_limit() -> Result<()> {
        // Very small limit
        let (mut writer, mut reader) = create_spill_channel(100);

        // Write a batch that exceeds the limit
        let large_batch = create_test_batch(0, 100);
        writer.push_batch(&large_batch)?;

        // Should still write and read successfully
        let result = reader.next().await.unwrap()?;
        assert_eq!(result.num_rows(), 100);

        // Next batch should go to a new file
        let batch2 = create_test_batch(100, 10);
        writer.push_batch(&batch2)?;

        let result2 = reader.next().await.unwrap()?;
        assert_eq!(result2.num_rows(), 10);

        Ok(())
    }

    #[tokio::test]
    async fn test_very_small_max_file_size() -> Result<()> {
        // Test with just 1 byte max (extreme case)
        let (mut writer, mut reader) = create_spill_channel(1);

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
        let (mut writer, mut reader) = create_spill_channel(batch_size);

        // Write two batches
        writer.push_batch(&batch)?;
        let batch2 = create_test_batch(10, 10);
        writer.push_batch(&batch2)?;

        // Both should be readable
        let result1 = reader.next().await.unwrap()?;
        assert_eq!(result1.num_rows(), 10);

        let result2 = reader.next().await.unwrap()?;
        assert_eq!(result2.num_rows(), 10);

        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_reader_writer() -> Result<()> {
        let (mut writer, mut reader) = create_spill_channel(1024 * 1024);

        // Spawn writer task
        let writer_handle = tokio::spawn(async move {
            for i in 0..10 {
                let batch = create_test_batch(i * 10, 10);
                writer.push_batch(&batch).unwrap();
                // Small delay to simulate real concurrent work
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        });

        // Reader task (runs concurrently)
        let reader_handle = tokio::spawn(async move {
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
        let (mut writer, mut reader) = create_spill_channel(1024 * 1024);

        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        enum ReadWriteEvent {
            ReadStart,
            Read(usize),
            Write(usize),
        }

        let events = Arc::new(Mutex::new(vec![]));
        // Start reader first (will pend)
        let reader_events = Arc::clone(&events);
        let reader_handle = tokio::spawn(async move {
            reader_events.lock().push(ReadWriteEvent::ReadStart);
            let result = reader.next().await.unwrap().unwrap();
            reader_events
                .lock()
                .push(ReadWriteEvent::Read(result.num_rows()));
            let result = reader.next().await.unwrap().unwrap();
            reader_events
                .lock()
                .push(ReadWriteEvent::Read(result.num_rows()));
        });

        // Give reader time to start pending
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        // Now write a batch (should wake the reader)
        let batch = create_test_batch(0, 5);
        events.lock().push(ReadWriteEvent::Write(batch.num_rows()));
        writer.push_batch(&batch)?;

        // Wait for the reader to process
        let processed = async {
            loop {
                if events.lock().len() >= 3 {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_micros(500)).await;
            }
        };
        tokio::time::timeout(std::time::Duration::from_secs(1), processed)
            .await
            .unwrap();

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
        let (mut writer, reader) = create_spill_channel(128);

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

        let (mut writer, mut reader) = channel(1024 * 1024, spill_manager);

        // Write some batches
        for i in 0..5 {
            let batch = create_test_batch(i * 10, 10);
            writer.push_batch(&batch)?;
        }

        // Check metrics before drop - spilled_bytes should be 0 since file isn't finalized yet
        let spilled_bytes_before = metrics.spilled_bytes.value();
        assert_eq!(
            spilled_bytes_before, 0,
            "Spilled bytes should be 0 before writer is dropped"
        );

        // Explicitly drop the writer - this should finalize the current file
        drop(writer);

        // Check metrics after drop - spilled_bytes should be > 0 now
        let spilled_bytes_after = metrics.spilled_bytes.value();
        assert!(
            spilled_bytes_after > 0,
            "Spilled bytes should be > 0 after writer is dropped (got {})",
            spilled_bytes_after
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
}
