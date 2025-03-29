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

use super::{spill_manager::SpillManager, IPCStreamWriter};

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

    /// Appends a `RecordBatch` to the file, initializing the writer if necessary.
    pub fn append_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.in_progress_file.is_none() {
            return Err(exec_datafusion_err!(
                "Append operation failed: No active in-progress file. The file may have already been finalized."
            ));
        }
        if self.writer.is_none() {
            let schema = batch.schema();
            if let Some(ref in_progress_file) = self.in_progress_file {
                self.writer = Some(IPCStreamWriter::new(
                    in_progress_file.path(),
                    schema.as_ref(),
                )?);

                // Update metrics
                self.spill_writer.metrics.spill_file_count.add(1);
            }
        }
        if let Some(writer) = &mut self.writer {
            let (spilled_rows, spilled_bytes) = writer.write(batch)?;

            // Update metrics
            self.spill_writer.metrics.spilled_bytes.add(spilled_bytes);
            self.spill_writer.metrics.spilled_rows.add(spilled_rows);
        }
        Ok(())
    }

    /// Finalizes the file, returning the completed file reference.
    /// If there are no batches spilled before, it returns `None`.
    pub fn finish(&mut self) -> Result<Option<RefCountedTempFile>> {
        if let Some(writer) = &mut self.writer {
            writer.finish()?;
        } else {
            return Ok(None);
        }

        Ok(self.in_progress_file.take())
    }
}
