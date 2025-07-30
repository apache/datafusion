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

//! Define the `SpillManager` struct, which is responsible for reading and writing `RecordBatch`es to raw files based on the provided configurations.

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_execution::runtime_env::RuntimeEnv;
use std::sync::Arc;

use datafusion_common::{config::SpillCompression, Result};
use datafusion_execution::disk_manager::RefCountedTempFile;
use datafusion_execution::SendableRecordBatchStream;

use super::{in_progress_spill_file::InProgressSpillFile, SpillReaderStream};
use crate::coop::cooperative;
use crate::{common::spawn_buffered, metrics::SpillMetrics};

/// The `SpillManager` is responsible for the following tasks:
/// - Reading and writing `RecordBatch`es to raw files based on the provided configurations.
/// - Updating the associated metrics.
///
/// Note: The caller (external operators such as `SortExec`) is responsible for interpreting the spilled files.
/// For example, all records within the same spill file are ordered according to a specific order.
#[derive(Debug, Clone)]
pub struct SpillManager {
    env: Arc<RuntimeEnv>,
    pub(crate) metrics: SpillMetrics,
    schema: SchemaRef,
    /// Number of batches to buffer in memory during disk reads
    batch_read_buffer_capacity: usize,
    /// general-purpose compression options
    pub(crate) compression: SpillCompression,
}

impl SpillManager {
    pub fn new(env: Arc<RuntimeEnv>, metrics: SpillMetrics, schema: SchemaRef) -> Self {
        Self {
            env,
            metrics,
            schema,
            batch_read_buffer_capacity: 2,
            compression: SpillCompression::default(),
        }
    }

    pub fn with_batch_read_buffer_capacity(
        mut self,
        batch_read_buffer_capacity: usize,
    ) -> Self {
        self.batch_read_buffer_capacity = batch_read_buffer_capacity;
        self
    }

    pub fn with_compression_type(mut self, spill_compression: SpillCompression) -> Self {
        self.compression = spill_compression;
        self
    }

    /// Creates a temporary file for in-progress operations, returning an error
    /// message if file creation fails. The file can be used to append batches
    /// incrementally and then finish the file when done.
    pub fn create_in_progress_file(
        &self,
        request_msg: &str,
    ) -> Result<InProgressSpillFile> {
        let temp_file = self.env.disk_manager.create_tmp_file(request_msg)?;
        Ok(InProgressSpillFile::new(Arc::new(self.clone()), temp_file))
    }

    /// Spill input `batches` into a single file in a atomic operation. If it is
    /// intended to incrementally write in-memory batches into the same spill file,
    /// use [`Self::create_in_progress_file`] instead.
    /// None is returned if no batches are spilled.
    ///
    /// # Errors
    /// - Returns an error if spilling would exceed the disk usage limit configured
    ///   by `max_temp_directory_size` in `DiskManager`
    pub fn spill_record_batch_and_finish(
        &self,
        batches: &[RecordBatch],
        request_msg: &str,
    ) -> Result<Option<RefCountedTempFile>> {
        let mut in_progress_file = self.create_in_progress_file(request_msg)?;

        for batch in batches {
            in_progress_file.append_batch(batch)?;
        }

        in_progress_file.finish()
    }

    /// Refer to the documentation for [`Self::spill_record_batch_and_finish`]. This method
    /// additionally spills the `RecordBatch` into smaller batches, divided by `row_limit`.
    ///
    /// # Errors
    /// - Returns an error if spilling would exceed the disk usage limit configured
    ///   by `max_temp_directory_size` in `DiskManager`
    pub fn spill_record_batch_by_size(
        &self,
        batch: &RecordBatch,
        request_description: &str,
        row_limit: usize,
    ) -> Result<Option<RefCountedTempFile>> {
        let total_rows = batch.num_rows();
        let mut batches = Vec::new();
        let mut offset = 0;

        // It's ok to calculate all slices first, because slicing is zero-copy.
        while offset < total_rows {
            let length = std::cmp::min(total_rows - offset, row_limit);
            let sliced_batch = batch.slice(offset, length);
            batches.push(sliced_batch);
            offset += length;
        }

        // Spill the sliced batches to disk
        self.spill_record_batch_and_finish(&batches, request_description)
    }

    /// Refer to the documentation for [`Self::spill_record_batch_and_finish`]. This method
    /// additionally spills the `RecordBatch` into smaller batches, divided by `row_limit`.
    ///
    /// # Errors
    /// - Returns an error if spilling would exceed the disk usage limit configured
    ///   by `max_temp_directory_size` in `DiskManager`
    pub(crate) fn spill_record_batch_by_size_and_return_max_batch_memory(
        &self,
        batch: &RecordBatch,
        request_description: &str,
        row_limit: usize,
    ) -> Result<Option<(RefCountedTempFile, usize)>> {
        let total_rows = batch.num_rows();
        let mut batches = Vec::new();
        let mut offset = 0;

        // It's ok to calculate all slices first, because slicing is zero-copy.
        while offset < total_rows {
            let length = std::cmp::min(total_rows - offset, row_limit);
            let sliced_batch = batch.slice(offset, length);
            batches.push(sliced_batch);
            offset += length;
        }

        let mut in_progress_file = self.create_in_progress_file(request_description)?;

        let mut max_record_batch_size = 0;

        for batch in batches {
            in_progress_file.append_batch(&batch)?;

            max_record_batch_size = max_record_batch_size.max(batch.get_sliced_size()?);
        }

        let file = in_progress_file.finish()?;

        Ok(file.map(|f| (f, max_record_batch_size)))
    }

    /// Spill a stream of `RecordBatch`es to disk and return the spill file and the size of the largest batch in memory
    pub(crate) async fn spill_record_batch_stream_and_return_max_batch_memory(
        &self,
        stream: &mut SendableRecordBatchStream,
        request_description: &str,
    ) -> Result<Option<(RefCountedTempFile, usize)>> {
        use futures::StreamExt;

        let mut in_progress_file = self.create_in_progress_file(request_description)?;

        let mut max_record_batch_size = 0;

        while let Some(batch) = stream.next().await {
            let batch = batch?;
            in_progress_file.append_batch(&batch)?;

            max_record_batch_size = max_record_batch_size.max(batch.get_sliced_size()?);
        }

        let file = in_progress_file.finish()?;

        Ok(file.map(|f| (f, max_record_batch_size)))
    }

    /// Reads a spill file as a stream. The file must be created by the current `SpillManager`.
    /// This method will generate output in FIFO order: the batch appended first
    /// will be read first.
    pub fn read_spill_as_stream(
        &self,
        spill_file_path: RefCountedTempFile,
    ) -> Result<SendableRecordBatchStream> {
        let stream = Box::pin(cooperative(SpillReaderStream::new(
            Arc::clone(&self.schema),
            spill_file_path,
        )));

        Ok(spawn_buffered(stream, self.batch_read_buffer_capacity))
    }
}

pub(crate) trait GetSlicedSize {
    /// Returns the size of the `RecordBatch` when sliced.
    fn get_sliced_size(&self) -> Result<usize>;
}

impl GetSlicedSize for RecordBatch {
    fn get_sliced_size(&self) -> Result<usize> {
        let mut total = 0;
        for array in self.columns() {
            let data = array.to_data();
            total += data.get_slice_memory_size()?;
        }
        Ok(total)
    }
}
