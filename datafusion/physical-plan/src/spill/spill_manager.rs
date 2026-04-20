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

use super::{SpillReaderStream, in_progress_spill_file::InProgressSpillFile};
use crate::coop::cooperative;
use crate::{common::spawn_buffered, metrics::SpillMetrics};
use arrow::array::{BinaryViewArray, GenericByteViewArray, StringViewArray};
use arrow::datatypes::{ByteViewType, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result, config::SpillCompression};
use datafusion_execution::SendableRecordBatchStream;
use datafusion_execution::disk_manager::RefCountedTempFile;
use datafusion_execution::runtime_env::RuntimeEnv;
use std::borrow::Borrow;
use std::sync::Arc;

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

    /// Returns the schema for batches managed by this SpillManager
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
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

    /// Spill an iterator of `RecordBatch`es to disk and return the spill file and the size of the largest batch in memory
    /// Note that this expects the caller to provide *non-sliced* batches, so the memory calculation of each batch is accurate.
    pub(crate) fn spill_record_batch_iter_and_return_max_batch_memory(
        &self,
        mut iter: impl Iterator<Item = Result<impl Borrow<RecordBatch>>>,
        request_description: &str,
    ) -> Result<Option<(RefCountedTempFile, usize)>> {
        let mut in_progress_file = self.create_in_progress_file(request_description)?;

        let mut max_record_batch_size = 0;

        iter.try_for_each(|batch| {
            let batch = batch?;
            let borrowed = batch.borrow();
            if borrowed.num_rows() == 0 {
                return Ok(());
            }
            let gc_sliced_size = in_progress_file.append_batch(borrowed)?;
            max_record_batch_size = max_record_batch_size.max(gc_sliced_size);
            Result::<_, DataFusionError>::Ok(())
        })?;

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
            let gc_sliced_size = in_progress_file.append_batch(&batch)?;

            max_record_batch_size = max_record_batch_size.max(gc_sliced_size);
        }

        let file = in_progress_file.finish()?;

        Ok(file.map(|f| (f, max_record_batch_size)))
    }

    /// Reads a spill file as a stream. The file must be created by the current
    /// `SpillManager`; otherwise an error will be returned.
    ///
    /// Output is produced in FIFO order: the batch appended first is read first.
    ///
    /// # Arg `max_record_batch_memory`
    ///
    /// Most callers should pass `None`. This is mainly useful for the
    /// memory-limited sort-preserving merge path.
    ///
    /// When provided, this value is used only as a validation hint. If a
    /// decoded batch exceeds this threshold, a debug-level log message is
    /// emitted.
    ///
    /// That path uses the maximum spilled batch size to conservatively estimate
    /// the merge degree when merging multiple sorted runs.
    pub fn read_spill_as_stream(
        &self,
        spill_file_path: RefCountedTempFile,
        max_record_batch_memory: Option<usize>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = Box::pin(cooperative(SpillReaderStream::new(
            Arc::clone(&self.schema),
            spill_file_path,
            max_record_batch_memory,
        )));

        Ok(spawn_buffered(stream, self.batch_read_buffer_capacity))
    }

    /// Same as `read_spill_as_stream`, but without buffering.
    pub fn read_spill_as_stream_unbuffered(
        &self,
        spill_file_path: RefCountedTempFile,
        max_record_batch_memory: Option<usize>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(cooperative(SpillReaderStream::new(
            Arc::clone(&self.schema),
            spill_file_path,
            max_record_batch_memory,
        ))))
    }
}

pub(crate) trait GetSlicedSize {
    /// Returns the size of the `RecordBatch` when sliced.
    /// Note: if multiple arrays or even a single array share the same data buffers, we may double count each buffer.
    /// Therefore, make sure we call gc() or gc_view_arrays() before using this method.
    fn get_sliced_size(&self) -> Result<usize>;
}

impl GetSlicedSize for RecordBatch {
    fn get_sliced_size(&self) -> Result<usize> {
        let mut total = 0;
        for array in self.columns() {
            let data = array.to_data();
            total += data.get_slice_memory_size()?;

            // While StringViewArray holds large data buffer for non inlined string, the Arrow layout (BufferSpec)
            // does not include any data buffers. Currently, ArrayData::get_slice_memory_size()
            // under-counts memory size by accounting only views buffer although data buffer is cloned during slice()
            //
            // Therefore, we manually add the sum of the lengths used by all non inlined views
            // on top of the sliced size for views buffer. This matches the intended semantics of
            // "bytes needed if we materialized exactly this slice into fresh buffers".
            // This is a workaround until https://github.com/apache/arrow-rs/issues/8230
            if let Some(sv) = array.as_any().downcast_ref::<StringViewArray>() {
                total += byte_view_data_buffer_size(sv);
            }
            if let Some(bv) = array.as_any().downcast_ref::<BinaryViewArray>() {
                total += byte_view_data_buffer_size(bv);
            }
        }
        Ok(total)
    }
}

fn byte_view_data_buffer_size<T: ByteViewType>(array: &GenericByteViewArray<T>) -> usize {
    array
        .data_buffers()
        .iter()
        .map(|buffer| buffer.capacity())
        .sum()
}

#[cfg(test)]
mod tests {
    use super::SpillManager;
    use crate::common::collect;
    use crate::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
    use crate::spill::{get_record_batch_memory_size, spill_manager::GetSlicedSize};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::{
        array::{ArrayRef, Int32Array, StringArray, StringViewArray},
        record_batch::RecordBatch,
    };
    use datafusion_common::Result;
    use datafusion_execution::runtime_env::RuntimeEnv;
    use std::sync::Arc;

    fn build_test_spill_manager(
        env: Arc<RuntimeEnv>,
        schema: Arc<Schema>,
    ) -> SpillManager {
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        SpillManager::new(env, metrics, schema)
    }

    fn build_writer_batch(schema: Arc<Schema>) -> Result<RecordBatch> {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .map_err(Into::into)
    }

    #[tokio::test]
    async fn test_read_spill_as_stream_from_another_spill_manager_same_schema()
    -> Result<()> {
        let env = Arc::new(RuntimeEnv::default());
        let writer_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let reader_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        let writer =
            build_test_spill_manager(Arc::clone(&env), Arc::clone(&writer_schema));
        let reader = build_test_spill_manager(env, Arc::clone(&reader_schema));
        let written_batch = build_writer_batch(Arc::clone(&writer_schema))?;

        let spill_file = writer
            .spill_record_batch_and_finish(
                std::slice::from_ref(&written_batch),
                "writer",
            )?
            .unwrap();

        // Same-schema reads through a different SpillManager currently pass
        // because only schema compatibility is validated. This is not a
        // supported usage pattern.
        let stream = reader.read_spill_as_stream(spill_file, None)?;
        assert_eq!(stream.schema(), reader_schema);

        let batches = collect(stream).await?;
        assert_eq!(batches, vec![written_batch]);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_spill_as_stream_from_another_spill_manager_different_schema()
    -> Result<()> {
        let env = Arc::new(RuntimeEnv::default());
        let writer_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let reader_schema = Arc::new(Schema::new(vec![
            Field::new("other_id", DataType::Int32, true),
            Field::new("other_value", DataType::Utf8, true),
        ]));

        let writer =
            build_test_spill_manager(Arc::clone(&env), Arc::clone(&writer_schema));
        let reader = build_test_spill_manager(env, Arc::clone(&reader_schema));
        let written_batch = build_writer_batch(Arc::clone(&writer_schema))?;

        let spill_file = writer
            .spill_record_batch_and_finish(
                std::slice::from_ref(&written_batch),
                "writer",
            )?
            .unwrap();

        let stream = reader.read_spill_as_stream(spill_file, None)?;
        let err = collect(stream)
            .await
            .expect_err("schema mismatch should fail fast");
        let err = err.to_string();
        assert!(err.contains("Spill file schema mismatch"));
        assert!(err.contains("expected"));
        assert!(err.contains("got"));

        Ok(())
    }

    #[test]
    fn check_sliced_size_for_string_view_array() -> Result<()> {
        let array_length = 50;
        let short_len = 8;
        let long_len = 25;

        // Build StringViewArray that includes both inline strings and non inlined strings
        let strings: Vec<String> = (0..array_length)
            .map(|i| {
                if i % 2 == 0 {
                    "a".repeat(short_len)
                } else {
                    "b".repeat(long_len)
                }
            })
            .collect();

        let string_array = StringViewArray::from(strings);
        let array_ref: ArrayRef = Arc::new(string_array);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "strings",
                DataType::Utf8View,
                false,
            )])),
            vec![array_ref],
        )
        .unwrap();

        // We did not slice the batch, so these two memory size should be equal
        assert_eq!(
            batch.get_sliced_size().unwrap(),
            get_record_batch_memory_size(&batch)
        );

        // Slice the batch into half
        let half_batch = batch.slice(0, array_length / 2);
        // Now sliced_size is smaller because the views buffer is sliced
        assert!(
            half_batch.get_sliced_size().unwrap()
                < get_record_batch_memory_size(&half_batch)
        );
        let data = arrow::array::Array::to_data(&half_batch.column(0));
        let views_sliced_size = data.get_slice_memory_size()?;
        // The sliced size should be larger than sliced views buffer size
        assert!(views_sliced_size < half_batch.get_sliced_size().unwrap());

        Ok(())
    }
}
