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
use datafusion_execution::spill_file::SpillFile;

use super::{
    IPCStreamWriter, gc_view_arrays,
    spill_manager::{GetSlicedSize, SpillManager},
};

/// Represents an in-progress spill file used for writing `RecordBatch`es to disk, created by `SpillManager`.
/// Caller is able to use this struct to incrementally append in-memory batches to
/// the file, and then finalize the file by calling the `finish` method.
pub struct InProgressSpillFile {
    pub(crate) spill_writer: Arc<SpillManager>,
    /// Lazily initialized writer
    writer: Option<IPCStreamWriter>,
    /// Lazily initialized in-progress file, it will be moved out when the `finish` method is invoked
    in_progress_file: Option<Arc<dyn SpillFile>>,
}

impl InProgressSpillFile {
    pub fn new(
        spill_writer: Arc<SpillManager>,
        in_progress_file: Arc<dyn SpillFile>,
    ) -> Self {
        Self {
            spill_writer,
            in_progress_file: Some(in_progress_file),
            writer: None,
        }
    }

    /// Appends a `RecordBatch` to the spill file, initializing the writer if necessary.
    ///
    /// Before writing, performs GC on StringView/BinaryView arrays to compact backing
    /// buffers. When a view array is sliced, it still references the original full buffers,
    /// causing massive spill files without GC (see issue #19414: 820MB → 33MB after GC).
    ///
    /// Returns the post-GC sliced memory size of the batch for memory accounting.
    ///
    /// # Errors
    /// - Returns an error if the file is not active (has been finalized)
    /// - Returns an error if appending would exceed the disk usage limit configured
    ///   by `max_temp_directory_size` in `DiskManager`
    pub fn append_batch(&mut self, batch: &RecordBatch) -> Result<usize> {
        if self.in_progress_file.is_none() {
            return Err(exec_datafusion_err!(
                "Append operation failed: No active in-progress file. The file may have already been finalized."
            ));
        }

        let gc_batch = gc_view_arrays(batch)?;

        if self.writer.is_none() {
            // Use the SpillManager's declared schema rather than the batch's schema.
            // Individual batches may have different schemas (e.g., different nullability)
            // when they come from different branches of a UnionExec. The SpillManager's
            // schema represents the canonical schema that all batches should conform to.
            let schema = self.spill_writer.schema();
            if let Some(in_progress_file) = &self.in_progress_file {
                let spill_writer = in_progress_file.open_writer()?;

                self.writer = Some(IPCStreamWriter::new(
                    spill_writer,
                    schema.as_ref(),
                    self.spill_writer.compression,
                )?);

                // Update metrics
                self.spill_writer.metrics.spill_file_count.add(1);
                let header_bytes = self.writer.as_ref().unwrap().bytes_written();
                self.spill_writer.metrics.spilled_bytes.add(header_bytes);
            }
        }
        if let Some(writer) = &mut self.writer {
            // The writer calculates how many serialized bytes were emitted
            let (spilled_rows, delta_bytes) = writer.write(&gc_batch)?;

            self.spill_writer.metrics.spilled_rows.add(spilled_rows);
            self.spill_writer.metrics.spilled_bytes.add(delta_bytes);
        }
        gc_batch.get_sliced_size()
    }

    pub fn flush(&mut self) -> Result<()> {
        if let Some(writer) = &mut self.writer {
            writer.flush()?;
        }
        Ok(())
    }

    /// Returns a reference to the in-progress file, if it exists.
    /// This can be used to get the file path for creating readers before the file is finished.
    pub fn file(&self) -> Option<&Arc<dyn SpillFile>> {
        self.in_progress_file.as_ref()
    }

    /// Finalizes the write process, returning the completed `SpillFile`.
    /// If there are no batches spilled before, it returns `None`.
    pub fn finish(&mut self) -> Result<Option<Arc<dyn SpillFile>>> {
        if self.in_progress_file.is_none() && self.writer.is_none() {
            return Err(exec_datafusion_err!(
                "Finish operation failed: file has already been finalized."
            ));
        }
        if let Some(mut writer) = self.writer.take() {
            // Finish the writer and capture any final trailing bytes emitted
            let delta_bytes = writer.finish()?;
            self.spill_writer.metrics.spilled_bytes.add(delta_bytes);
        } else {
            return Ok(None);
        }

        Ok(self.in_progress_file.take())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_physical_expr_common::metrics::{
        ExecutionPlanMetricsSet, SpillMetrics,
    };
    use futures::TryStreamExt;

    #[tokio::test]
    async fn test_spill_file_uses_spill_manager_schema() -> Result<()> {
        let nullable_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("val", DataType::Int64, true),
        ]));
        let non_nullable_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("val", DataType::Int64, false),
        ]));

        let runtime = Arc::new(RuntimeEnvBuilder::new().build()?);
        let metrics_set = ExecutionPlanMetricsSet::new();
        let spill_metrics = SpillMetrics::new(&metrics_set, 0);
        let spill_manager = Arc::new(SpillManager::new(
            runtime,
            spill_metrics,
            Arc::clone(&nullable_schema),
        ));

        let mut in_progress = spill_manager.create_in_progress_file("test")?;

        // First batch: non-nullable val (simulates literal-0 UNION branch)
        let non_nullable_batch = RecordBatch::try_new(
            Arc::clone(&non_nullable_schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![0, 0, 0])),
            ],
        )?;
        in_progress.append_batch(&non_nullable_batch)?;

        // Second batch: nullable val with NULLs (simulates table UNION branch)
        let nullable_batch = RecordBatch::try_new(
            Arc::clone(&nullable_schema),
            vec![
                Arc::new(Int64Array::from(vec![4, 5, 6])),
                Arc::new(Int64Array::from(vec![Some(10), None, Some(30)])),
            ],
        )?;
        in_progress.append_batch(&nullable_batch)?;

        let spill_file = in_progress.finish()?.unwrap();

        let stream = spill_manager.read_spill_as_stream(spill_file, None)?;

        // Stream schema should be nullable
        assert_eq!(stream.schema(), nullable_schema);

        let batches = stream.try_collect::<Vec<_>>().await?;
        assert_eq!(batches.len(), 2);

        // Both batches must have the SpillManager's nullable schema
        assert_eq!(
            batches[0],
            non_nullable_batch.with_schema(Arc::clone(&nullable_schema))?
        );
        assert_eq!(batches[1], nullable_batch);

        Ok(())
    }
}
