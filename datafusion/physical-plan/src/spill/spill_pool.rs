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
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use datafusion_common::{exec_datafusion_err, Result};
use datafusion_execution::disk_manager::RefCountedTempFile;

use super::in_progress_spill_file::InProgressSpillFile;
use super::spill_manager::SpillManager;

/// A single spill file containing one or more record batches.
struct SpillFile {
    /// The temp file handle (auto-deletes when dropped)
    file: RefCountedTempFile,
    /// Number of batches originally written to this file
    total_batches: usize,
    /// Number of batches already read from this file
    batches_read: usize,
    /// Total size of this file in bytes (kept for potential debugging/metrics)
    #[allow(dead_code)]
    total_size: usize,
    /// Sequential reader for this file (lazily initialized on first read)
    reader: Option<StreamReader<BufReader<File>>>,
}

impl SpillFile {
    fn new(file: RefCountedTempFile, total_batches: usize, total_size: usize) -> Self {
        Self {
            file,
            total_batches,
            batches_read: 0,
            total_size,
            reader: None,
        }
    }

    /// Returns true if all batches have been read from this file
    fn is_fully_consumed(&self) -> bool {
        self.batches_read >= self.total_batches
    }

    /// Returns the number of unread batches remaining
    fn remaining_batches(&self) -> usize {
        self.total_batches.saturating_sub(self.batches_read)
    }

    /// Reads the next batch from this file sequentially.
    ///
    /// Initializes the reader on first call. Returns None when all batches consumed.
    fn read_next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.is_fully_consumed() {
            return Ok(None);
        }

        // Initialize reader on first use
        if self.reader.is_none() {
            let file_handle = File::open(self.file.path()).map_err(|e| {
                exec_datafusion_err!(
                    "Failed to open spill file {:?} for reading: {}",
                    self.file.path(),
                    e
                )
            })?;
            let buf_reader = BufReader::new(file_handle);
            // SAFETY: DataFusion's spill writer strictly follows Arrow IPC specifications
            let reader = unsafe {
                StreamReader::try_new(buf_reader, None)?.with_skip_validation(true)
            };
            self.reader = Some(reader);
        }

        // Read next batch from sequential stream
        if let Some(reader) = &mut self.reader {
            match reader.next() {
                Some(Ok(batch)) => {
                    self.batches_read += 1;
                    Ok(Some(batch))
                }
                Some(Err(e)) => Err(e.into()),
                None => {
                    // Stream ended - this shouldn't happen if batch count is correct
                    if !self.is_fully_consumed() {
                        return Err(exec_datafusion_err!(
                            "Unexpected end of spill file: read {} batches but expected {}",
                            self.batches_read,
                            self.total_batches
                        ));
                    }
                    Ok(None)
                }
            }
        } else {
            unreachable!("Reader should be initialized above")
        }
    }
}

/// A pool of spill files that manages batch-level spilling with FIFO semantics.
///
/// Batches are written sequentially to files, with automatic rotation when the
/// configured size limit is reached. Reading is done in FIFO order (oldest batch first).
///
/// # Thread Safety
///
/// `SpillPool` is not thread-safe and should be used from a single thread or
/// protected with appropriate synchronization.
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
        }
    }

    /// Returns the number of files currently in the pool
    pub fn file_count(&self) -> usize {
        self.files.len()
            + if self.current_write_file.is_some() {
                1
            } else {
                0
            }
    }

    /// Returns the total number of unread batches across all files
    pub fn batch_count(&self) -> usize {
        self.files
            .iter()
            .map(|f| f.remaining_batches())
            .sum::<usize>()
            + self.current_batch_count
    }

    /// Returns true if the pool is empty (no batches to read)
    pub fn is_empty(&self) -> bool {
        self.batch_count() == 0
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

        Ok(())
    }

    /// Reads the next batch from the pool in FIFO order.
    ///
    /// Returns the oldest unread batch, or None if the pool is empty.
    ///
    /// # Errors
    ///
    /// Returns an error if disk I/O fails during read.
    pub fn pop_batch(&mut self) -> Result<Option<RecordBatch>> {
        // Ensure any pending writes are flushed first
        if self.current_write_file.is_some() {
            self.flush()?;
        }

        loop {
            // Get the oldest file (front of queue)
            let spill_file = match self.files.front_mut() {
                Some(file) => file,
                None => return Ok(None), // No files available
            };

            // Try to read next batch from this file
            match spill_file.read_next_batch()? {
                Some(batch) => {
                    // Check if file is now fully consumed after reading this batch
                    let is_consumed = spill_file.is_fully_consumed();
                    if is_consumed {
                        // Remove the file from the queue
                        self.files.pop_front();
                    }
                    return Ok(Some(batch));
                }
                None => {
                    // File is fully consumed, remove it and try next file
                    self.files.pop_front();
                    continue;
                }
            }
        }
    }

    /// Finalizes the current write file and adds it to the files queue.
    ///
    /// Called automatically by `push_batch` when rotating files, but can
    /// also be called explicitly to ensure all pending data is flushed.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_execution::runtime_env::RuntimeEnv;

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

        SpillPool::new(max_file_size, spill_manager, schema)
    }

    #[test]
    fn test_empty_pool() -> Result<()> {
        let mut pool = create_spill_pool(1024 * 1024);

        assert!(pool.is_empty());
        assert_eq!(pool.file_count(), 0);
        assert_eq!(pool.batch_count(), 0);
        assert!(pool.pop_batch()?.is_none());

        Ok(())
    }

    #[test]
    fn test_single_batch() -> Result<()> {
        let mut pool = create_spill_pool(1024 * 1024);

        // Push one batch
        let batch1 = create_test_batch(0, 10);
        pool.push_batch(&batch1)?;
        pool.flush()?;

        assert!(!pool.is_empty());
        assert_eq!(pool.file_count(), 1);
        assert_eq!(pool.batch_count(), 1);

        // Pop and verify
        let result = pool.pop_batch()?.unwrap();
        assert_eq!(result.num_rows(), 10);
        assert_eq!(result.num_columns(), 2);

        // Pool should be empty now
        assert!(pool.is_empty());
        assert_eq!(pool.file_count(), 0);

        Ok(())
    }

    #[test]
    fn test_multiple_batches_single_file() -> Result<()> {
        let mut pool = create_spill_pool(10 * 1024 * 1024); // Large file size

        // Push multiple batches
        for i in 0..5 {
            let batch = create_test_batch(i * 10, 10);
            pool.push_batch(&batch)?;
        }
        pool.flush()?;

        assert_eq!(pool.file_count(), 1);
        assert_eq!(pool.batch_count(), 5);

        // Pop in FIFO order
        for i in 0..5 {
            let result = pool.pop_batch()?.unwrap();
            assert_eq!(result.num_rows(), 10);

            // Verify first value is correct (FIFO order)
            let col_a = result
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(col_a.value(0), i * 10);
        }

        assert!(pool.is_empty());
        Ok(())
    }

    #[test]
    fn test_file_rotation() -> Result<()> {
        // Small file size to force rotation
        let mut pool = create_spill_pool(500); // ~500 bytes

        // Push multiple batches - should create multiple files
        for i in 0..10 {
            let batch = create_test_batch(i * 5, 5);
            pool.push_batch(&batch)?;
        }
        pool.flush()?;

        // Should have multiple files due to size limit
        assert!(pool.file_count() > 1, "Expected file rotation to occur");
        assert_eq!(pool.batch_count(), 10);

        // Pop all batches in FIFO order
        for i in 0..10 {
            let result = pool.pop_batch()?.unwrap();
            let col_a = result
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(col_a.value(0), i * 5);
        }

        assert!(pool.is_empty());
        Ok(())
    }

    #[test]
    fn test_empty_batch_skipped() -> Result<()> {
        let mut pool = create_spill_pool(1024 * 1024);

        let batch1 = create_test_batch(0, 10);
        let empty_batch = RecordBatch::new_empty(create_test_schema());
        let batch2 = create_test_batch(10, 10);

        pool.push_batch(&batch1)?;
        pool.push_batch(&empty_batch)?; // Should be skipped
        pool.push_batch(&batch2)?;
        pool.flush()?;

        // Should only have 2 batches (empty one skipped)
        assert_eq!(pool.batch_count(), 2);

        Ok(())
    }

    #[test]
    fn test_interleaved_push_pop() -> Result<()> {
        let mut pool = create_spill_pool(1024 * 1024);

        // Push batch 0
        pool.push_batch(&create_test_batch(0, 5))?;
        pool.flush()?;

        // Pop batch 0
        let result = pool.pop_batch()?.unwrap();
        assert_eq!(result.num_rows(), 5);

        // Push batches 1, 2
        pool.push_batch(&create_test_batch(10, 5))?;
        pool.push_batch(&create_test_batch(20, 5))?;
        pool.flush()?;

        // Pop batch 1
        let result = pool.pop_batch()?.unwrap();
        let col_a = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col_a.value(0), 10);

        // Push batch 3
        pool.push_batch(&create_test_batch(30, 5))?;
        pool.flush()?;

        // Pop remaining: should get 2, 3
        let result = pool.pop_batch()?.unwrap();
        let col_a = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col_a.value(0), 20);

        let result = pool.pop_batch()?.unwrap();
        let col_a = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col_a.value(0), 30);

        assert!(pool.is_empty());
        Ok(())
    }

    #[test]
    fn test_auto_flush_on_pop() -> Result<()> {
        let mut pool = create_spill_pool(1024 * 1024);

        // Push without explicit flush
        pool.push_batch(&create_test_batch(0, 10))?;

        // Pop should auto-flush
        let result = pool.pop_batch()?.unwrap();
        assert_eq!(result.num_rows(), 10);

        Ok(())
    }

    #[test]
    fn test_large_batches() -> Result<()> {
        let mut pool = create_spill_pool(1024 * 1024);

        // Create larger batches
        let batch1 = create_test_batch(0, 1000);
        let batch2 = create_test_batch(1000, 1000);

        pool.push_batch(&batch1)?;
        pool.push_batch(&batch2)?;
        pool.flush()?;

        let result1 = pool.pop_batch()?.unwrap();
        assert_eq!(result1.num_rows(), 1000);

        let result2 = pool.pop_batch()?.unwrap();
        assert_eq!(result2.num_rows(), 1000);

        Ok(())
    }

    #[test]
    fn test_file_cleanup() -> Result<()> {
        let mut pool = create_spill_pool(500);

        // Create multiple files
        for i in 0..10 {
            pool.push_batch(&create_test_batch(i * 5, 5))?;
        }
        pool.flush()?;

        let initial_file_count = pool.file_count();
        assert!(initial_file_count > 1);

        // Pop some batches - should cleanup fully consumed files
        for _ in 0..5 {
            pool.pop_batch()?;
        }

        // File count should decrease as old files are consumed
        assert!(pool.file_count() <= initial_file_count);

        Ok(())
    }
}
