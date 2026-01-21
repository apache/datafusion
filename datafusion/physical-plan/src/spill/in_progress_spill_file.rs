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

//! Define the `InProgressSpillFile` struct, which represents an in-progress spill file used for writing `RecordBatch`es to disk, created by `SpillManager`.

use datafusion_common::Result;
use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion_common::exec_datafusion_err;
use datafusion_execution::disk_manager::RefCountedTempFile;

use super::{IPCStreamWriter, spill_manager::SpillManager};

/// Represents an in-progress spill file used for writing `RecordBatch`es to disk, created by `SpillManager`.
/// Caller is able to use this struct to incrementally append in-memory batches to
/// the file, and then finalize the file by calling the `finish` method.
pub struct InProgressSpillFile {
    pub(crate) spill_writer: Arc<SpillManager>,
    /// Lazily initialized writer
    writer: Option<IPCStreamWriter>,
    /// Lazily initialized in-progress file, it will be moved out when the `finish` method is invoked
    in_progress_file: Option<RefCountedTempFile>,
}

impl InProgressSpillFile {
    pub fn new(
        spill_writer: Arc<SpillManager>,
        in_progress_file: RefCountedTempFile,
    ) -> Self {
        Self {
            spill_writer,
            in_progress_file: Some(in_progress_file),
            writer: None,
        }
    }

    /// Appends a `RecordBatch` to the spill file, initializing the writer if necessary.
    ///
    /// # Errors
    /// - Returns an error if the file is not active (has been finalized)
    /// - Returns an error if appending would exceed the disk usage limit configured
    ///   by `max_temp_directory_size` in `DiskManager`
    pub fn append_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.in_progress_file.is_none() {
            return Err(exec_datafusion_err!(
                "Append operation failed: No active in-progress file. The file may have already been finalized."
            ));
        }
        if self.writer.is_none() {
            let schema = batch.schema();
            if let Some(in_progress_file) = &mut self.in_progress_file {
                self.writer = Some(IPCStreamWriter::new(
                    in_progress_file.path(),
                    schema.as_ref(),
                    self.spill_writer.compression,
                )?);

                // Update metrics
                self.spill_writer.metrics.spill_file_count.add(1);

                // Update initial size (schema/header)
                in_progress_file.update_disk_usage()?;
                let initial_size = in_progress_file.current_disk_usage();
                self.spill_writer
                    .metrics
                    .spilled_bytes
                    .add(initial_size as usize);
            }
        }
        if let Some(writer) = &mut self.writer {
            let (spilled_rows, _) = writer.write(batch)?;
            if let Some(in_progress_file) = &mut self.in_progress_file {
                let pre_size = in_progress_file.current_disk_usage();
                in_progress_file.update_disk_usage()?;
                let post_size = in_progress_file.current_disk_usage();

                self.spill_writer.metrics.spilled_rows.add(spilled_rows);
                self.spill_writer
                    .metrics
                    .spilled_bytes
                    .add((post_size - pre_size) as usize);
            } else {
                unreachable!() // Already checked inside current function
            }
        }
        Ok(())
    }

    /// Returns a reference to the in-progress file, if it exists.
    /// This can be used to get the file path for creating readers before the file is finished.
    pub fn file(&self) -> Option<&RefCountedTempFile> {
        self.in_progress_file.as_ref()
    }

    /// Finalizes the file, returning the completed file reference.
    /// If there are no batches spilled before, it returns `None`.
    pub fn finish(&mut self) -> Result<Option<RefCountedTempFile>> {
        if let Some(writer) = &mut self.writer {
            writer.finish()?;
        } else {
            return Ok(None);
        }

        // Since spill files are append-only, add the file size to spilled_bytes
        if let Some(in_progress_file) = &mut self.in_progress_file {
            // Since writer.finish() writes continuation marker and message length at the end
            let pre_size = in_progress_file.current_disk_usage();
            in_progress_file.update_disk_usage()?;
            let post_size = in_progress_file.current_disk_usage();
            self.spill_writer
                .metrics
                .spilled_bytes
                .add((post_size - pre_size) as usize);
        }

        Ok(self.in_progress_file.take())
    }
}
