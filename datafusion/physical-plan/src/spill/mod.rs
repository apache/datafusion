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

//! Defines the spilling functions

pub(crate) mod in_progress_spill_file;
pub(crate) mod spill_manager;
pub mod spill_pool;

// Re-export SpillManager for doctests only (hidden from public docs)
#[doc(hidden)]
pub use spill_manager::SpillManager;

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{ArrayData, BufferSpec, layout};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::ipc::{
    MetadataVersion,
    reader::StreamReader,
    writer::{IpcWriteOptions, StreamWriter},
};
use arrow::record_batch::RecordBatch;

use datafusion_common::config::SpillCompression;
use datafusion_common::{DataFusionError, HashSet, Result, exec_datafusion_err};
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::RecordBatchStream;
use datafusion_execution::disk_manager::RefCountedTempFile;
use futures::{FutureExt as _, Stream};
use log::warn;

/// Stream that reads spill files from disk where each batch is read in a spawned blocking task
/// It will read one batch at a time and will not do any buffering, to buffer data use [`crate::common::spawn_buffered`]
///
/// A simpler solution would be spawning a long-running blocking task for each
/// file read (instead of each batch). This approach does not work because when
/// the number of concurrent reads exceeds the Tokio thread pool limit,
/// deadlocks can occur and block progress.
struct SpillReaderStream {
    schema: SchemaRef,
    state: SpillReaderStreamState,
    /// Maximum memory size observed among spilling sorted record batches.
    /// This is used for validation purposes during reading each RecordBatch from spill.
    /// For context on why this value is recorded and validated,
    /// see `physical_plan/sort/multi_level_merge.rs`.
    max_record_batch_memory: Option<usize>,
}

// Small margin allowed to accommodate slight memory accounting variation
const SPILL_BATCH_MEMORY_MARGIN: usize = 4096;

/// When we poll for the next batch, we will get back both the batch and the reader,
/// so we can call `next` again.
type NextRecordBatchResult = Result<(StreamReader<BufReader<File>>, Option<RecordBatch>)>;

enum SpillReaderStreamState {
    /// Initial state: the stream was not initialized yet
    /// and the file was not opened
    Uninitialized(RefCountedTempFile),

    /// A read is in progress in a spawned blocking task for which we hold the handle.
    ReadInProgress(SpawnedTask<NextRecordBatchResult>),

    /// A read has finished and we wait for being polled again in order to start reading the next batch.
    Waiting(StreamReader<BufReader<File>>),

    /// The stream has finished, successfully or not.
    Done,
}

impl SpillReaderStream {
    fn new(
        schema: SchemaRef,
        spill_file: RefCountedTempFile,
        max_record_batch_memory: Option<usize>,
    ) -> Self {
        Self {
            schema,
            state: SpillReaderStreamState::Uninitialized(spill_file),
            max_record_batch_memory,
        }
    }

    fn poll_next_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        match &mut self.state {
            SpillReaderStreamState::Uninitialized(_) => {
                // Temporarily replace with `Done` to be able to pass the file to the task.
                let SpillReaderStreamState::Uninitialized(spill_file) =
                    std::mem::replace(&mut self.state, SpillReaderStreamState::Done)
                else {
                    unreachable!()
                };

                let task = SpawnedTask::spawn_blocking(move || {
                    let file = BufReader::new(File::open(spill_file.path())?);
                    // SAFETY: DataFusion's spill writer strictly follows Arrow IPC specifications
                    // with validated schemas and buffers. Skip redundant validation during read
                    // to speedup read operation. This is safe for DataFusion as input guaranteed to be correct when written.
                    let mut reader = unsafe {
                        StreamReader::try_new(file, None)?.with_skip_validation(true)
                    };

                    let next_batch = reader.next().transpose()?;

                    Ok((reader, next_batch))
                });

                self.state = SpillReaderStreamState::ReadInProgress(task);

                // Poll again immediately so the inner task is polled and the waker is
                // registered.
                self.poll_next_inner(cx)
            }

            SpillReaderStreamState::ReadInProgress(task) => {
                let result = futures::ready!(task.poll_unpin(cx))
                    .unwrap_or_else(|err| Err(DataFusionError::External(Box::new(err))));

                match result {
                    Ok((reader, batch)) => {
                        match batch {
                            Some(batch) => {
                                if let Some(max_record_batch_memory) =
                                    self.max_record_batch_memory
                                {
                                    let actual_size =
                                        get_record_batch_memory_size(&batch);
                                    if actual_size
                                        > max_record_batch_memory
                                            + SPILL_BATCH_MEMORY_MARGIN
                                    {
                                        warn!(
                                            "Record batch memory usage ({actual_size} bytes) exceeds the expected limit ({max_record_batch_memory} bytes) \n\
                                                by more than the allowed tolerance ({SPILL_BATCH_MEMORY_MARGIN} bytes).\n\
                                                This likely indicates a bug in memory accounting during spilling.\n\
                                                Please report this issue in https://github.com/apache/datafusion/issues/17340."
                                        );
                                    }
                                }
                                self.state = SpillReaderStreamState::Waiting(reader);

                                Poll::Ready(Some(Ok(batch)))
                            }
                            None => {
                                // Stream is done
                                self.state = SpillReaderStreamState::Done;

                                Poll::Ready(None)
                            }
                        }
                    }
                    Err(err) => {
                        self.state = SpillReaderStreamState::Done;

                        Poll::Ready(Some(Err(err)))
                    }
                }
            }

            SpillReaderStreamState::Waiting(_) => {
                // Temporarily replace with `Done` to be able to pass the file to the task.
                let SpillReaderStreamState::Waiting(mut reader) =
                    std::mem::replace(&mut self.state, SpillReaderStreamState::Done)
                else {
                    unreachable!()
                };

                let task = SpawnedTask::spawn_blocking(move || {
                    let next_batch = reader.next().transpose()?;

                    Ok((reader, next_batch))
                });

                self.state = SpillReaderStreamState::ReadInProgress(task);

                // Poll again immediately so the inner task is polled and the waker is
                // registered.
                self.poll_next_inner(cx)
            }

            SpillReaderStreamState::Done => Poll::Ready(None),
        }
    }
}

impl Stream for SpillReaderStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().poll_next_inner(cx)
    }
}

impl RecordBatchStream for SpillReaderStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Spill the `RecordBatch` to disk as smaller batches
/// split by `batch_size_rows`
#[deprecated(
    since = "46.0.0",
    note = "This method is deprecated. Use `SpillManager::spill_record_batch_by_size` instead."
)]
#[expect(clippy::needless_pass_by_value)]
pub fn spill_record_batch_by_size(
    batch: &RecordBatch,
    path: PathBuf,
    schema: SchemaRef,
    batch_size_rows: usize,
) -> Result<()> {
    let mut offset = 0;
    let total_rows = batch.num_rows();
    let mut writer =
        IPCStreamWriter::new(&path, schema.as_ref(), SpillCompression::Uncompressed)?;

    while offset < total_rows {
        let length = std::cmp::min(total_rows - offset, batch_size_rows);
        let batch = batch.slice(offset, length);
        offset += batch.num_rows();
        writer.write(&batch)?;
    }
    writer.finish()?;

    Ok(())
}

/// Calculate total used memory of this batch.
///
/// This function is used to estimate the physical memory usage of the `RecordBatch`.
/// It only counts the memory of large data `Buffer`s, and ignores metadata like
/// types and pointers.
/// The implementation will add up all unique `Buffer`'s memory
/// size, due to:
/// - The data pointer inside `Buffer` are memory regions returned by global memory
///   allocator, those regions can't have overlap.
/// - The actual used range of `ArrayRef`s inside `RecordBatch` can have overlap
///   or reuse the same `Buffer`. For example: taking a slice from `Array`.
///
/// Example:
/// For a `RecordBatch` with two columns: `col1` and `col2`, two columns are pointing
/// to a sub-region of the same buffer.
///
/// {xxxxxxxxxxxxxxxxxxx} <--- buffer
///       ^    ^  ^    ^
///       |    |  |    |
/// col1->{    }  |    |
/// col2--------->{    }
///
/// In the above case, `get_record_batch_memory_size` will return the size of
/// the buffer, instead of the sum of `col1` and `col2`'s actual memory size.
///
/// Note: Current `RecordBatch`.get_array_memory_size()` will double count the
/// buffer memory size if multiple arrays within the batch are sharing the same
/// `Buffer`. This method provides temporary fix until the issue is resolved:
/// <https://github.com/apache/arrow-rs/issues/6439>
pub fn get_record_batch_memory_size(batch: &RecordBatch) -> usize {
    // Store pointers to `Buffer`'s start memory address (instead of actual
    // used data region's pointer represented by current `Array`)
    let mut counted_buffers: HashSet<NonNull<u8>> = HashSet::new();
    let mut total_size = 0;

    for array in batch.columns() {
        let array_data = array.to_data();
        count_array_data_memory_size(&array_data, &mut counted_buffers, &mut total_size);
    }

    total_size
}

/// Count the memory usage of `array_data` and its children recursively.
fn count_array_data_memory_size(
    array_data: &ArrayData,
    counted_buffers: &mut HashSet<NonNull<u8>>,
    total_size: &mut usize,
) {
    // Count memory usage for `array_data`
    for buffer in array_data.buffers() {
        if counted_buffers.insert(buffer.data_ptr()) {
            *total_size += buffer.capacity();
        } // Otherwise the buffer's memory is already counted
    }

    if let Some(null_buffer) = array_data.nulls()
        && counted_buffers.insert(null_buffer.inner().inner().data_ptr())
    {
        *total_size += null_buffer.inner().inner().capacity();
    }

    // Count all children `ArrayData` recursively
    for child in array_data.child_data() {
        count_array_data_memory_size(child, counted_buffers, total_size);
    }
}

/// Write in Arrow IPC Stream format to a file.
///
/// Stream format is used for spill because it supports dictionary replacement, and the random
/// access of IPC File format is not needed (IPC File format doesn't support dictionary replacement).
struct IPCStreamWriter {
    /// Inner writer
    pub writer: StreamWriter<File>,
    /// Batches written
    pub num_batches: usize,
    /// Rows written
    pub num_rows: usize,
    /// Bytes written
    pub num_bytes: usize,
}

impl IPCStreamWriter {
    /// Create new writer
    pub fn new(
        path: &Path,
        schema: &Schema,
        compression_type: SpillCompression,
    ) -> Result<Self> {
        let file = File::create(path).map_err(|e| {
            exec_datafusion_err!("(Hint: you may increase the file descriptor limit with shell command 'ulimit -n 4096') Failed to create partition file at {path:?}: {e:?}")
        })?;

        let metadata_version = MetadataVersion::V5;
        // Depending on the schema, some array types such as StringViewArray require larger (16 byte in this case) alignment.
        // If the actual buffer layout after IPC read does not satisfy the alignment requirement,
        // Arrow ArrayBuilder will copy the buffer into a newly allocated, properly aligned buffer.
        // This copying may lead to memory blowup during IPC read due to duplicated buffers.
        // To avoid this, we compute the maximum required alignment based on the schema and configure the IPCStreamWriter accordingly.
        let alignment = get_max_alignment_for_schema(schema);
        let mut write_options =
            IpcWriteOptions::try_new(alignment, false, metadata_version)?;
        write_options = write_options.try_with_compression(compression_type.into())?;

        let writer = StreamWriter::try_new_with_options(file, schema, write_options)?;
        Ok(Self {
            num_batches: 0,
            num_rows: 0,
            num_bytes: 0,
            writer,
        })
    }

    /// Writes a single batch to the IPC stream and updates the internal counters.
    ///
    /// Returns a tuple containing the change in the number of rows and bytes written.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(usize, usize)> {
        self.writer.write(batch)?;
        self.num_batches += 1;
        let delta_num_rows = batch.num_rows();
        self.num_rows += delta_num_rows;
        let delta_num_bytes: usize = batch.get_array_memory_size();
        self.num_bytes += delta_num_bytes;
        Ok((delta_num_rows, delta_num_bytes))
    }

    /// Finish the writer
    pub fn finish(&mut self) -> Result<()> {
        self.writer.finish().map_err(Into::into)
    }
}

// Returns the maximum byte alignment required by any field in the schema (>= 8), derived from Arrow buffer layouts.
fn get_max_alignment_for_schema(schema: &Schema) -> usize {
    let minimum_alignment = 8;
    let mut max_alignment = minimum_alignment;
    for field in schema.fields() {
        let layout = layout(field.data_type());
        let required_alignment = layout
            .buffers
            .iter()
            .map(|buffer_spec| {
                if let BufferSpec::FixedWidth { alignment, .. } = buffer_spec {
                    *alignment
                } else {
                    minimum_alignment
                }
            })
            .max()
            .unwrap_or(minimum_alignment);
        max_alignment = std::cmp::max(max_alignment, required_alignment);
    }
    max_alignment
}

#[cfg(test)]
mod tests {
    use super::in_progress_spill_file::InProgressSpillFile;
    use super::*;
    use crate::common::collect;
    use crate::metrics::ExecutionPlanMetricsSet;
    use crate::metrics::SpillMetrics;
    use crate::spill::spill_manager::SpillManager;
    use crate::test::build_table_i32;
    use arrow::array::{ArrayRef, Float64Array, Int32Array, ListArray, StringArray};
    use arrow::compute::cast;
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion_common::Result;
    use datafusion_execution::runtime_env::RuntimeEnv;
    use futures::StreamExt as _;

    use std::sync::Arc;

    #[tokio::test]
    async fn test_batch_spill_and_read() -> Result<()> {
        let batch1 = build_table_i32(
            ("a2", &vec![0, 1, 2]),
            ("b2", &vec![3, 4, 5]),
            ("c2", &vec![4, 5, 6]),
        );

        let batch2 = build_table_i32(
            ("a2", &vec![10, 11, 12]),
            ("b2", &vec![13, 14, 15]),
            ("c2", &vec![14, 15, 16]),
        );

        let schema = batch1.schema();
        let num_rows = batch1.num_rows() + batch2.num_rows();

        // Construct SpillManager
        let env = Arc::new(RuntimeEnv::default());
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let spill_manager = SpillManager::new(env, metrics, Arc::clone(&schema));

        let spill_file = spill_manager
            .spill_record_batch_and_finish(&[batch1, batch2], "Test")?
            .unwrap();
        assert!(spill_file.path().exists());
        let spilled_rows = spill_manager.metrics.spilled_rows.value();
        assert_eq!(spilled_rows, num_rows);

        let stream = spill_manager.read_spill_as_stream(spill_file, None)?;
        assert_eq!(stream.schema(), schema);

        let batches = collect(stream).await?;
        assert_eq!(batches.len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_batch_spill_and_read_dictionary_arrays() -> Result<()> {
        // See https://github.com/apache/datafusion/issues/4658

        let batch1 = build_table_i32(
            ("a2", &vec![0, 1, 2]),
            ("b2", &vec![3, 4, 5]),
            ("c2", &vec![4, 5, 6]),
        );

        let batch2 = build_table_i32(
            ("a2", &vec![10, 11, 12]),
            ("b2", &vec![13, 14, 15]),
            ("c2", &vec![14, 15, 16]),
        );

        // Dictionary encode the arrays
        let dict_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32));
        let dict_schema = Arc::new(Schema::new(vec![
            Field::new("a2", dict_type.clone(), true),
            Field::new("b2", dict_type.clone(), true),
            Field::new("c2", dict_type.clone(), true),
        ]));

        let batch1 = RecordBatch::try_new(
            Arc::clone(&dict_schema),
            batch1
                .columns()
                .iter()
                .map(|array| cast(array, &dict_type))
                .collect::<Result<_, _>>()?,
        )?;

        let batch2 = RecordBatch::try_new(
            Arc::clone(&dict_schema),
            batch2
                .columns()
                .iter()
                .map(|array| cast(array, &dict_type))
                .collect::<Result<_, _>>()?,
        )?;

        // Construct SpillManager
        let env = Arc::new(RuntimeEnv::default());
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let spill_manager = SpillManager::new(env, metrics, Arc::clone(&dict_schema));

        let num_rows = batch1.num_rows() + batch2.num_rows();
        let spill_file = spill_manager
            .spill_record_batch_and_finish(&[batch1, batch2], "Test")?
            .unwrap();
        let spilled_rows = spill_manager.metrics.spilled_rows.value();
        assert_eq!(spilled_rows, num_rows);

        let stream = spill_manager.read_spill_as_stream(spill_file, None)?;
        assert_eq!(stream.schema(), dict_schema);
        let batches = collect(stream).await?;
        assert_eq!(batches.len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_batch_spill_by_size() -> Result<()> {
        let batch1 = build_table_i32(
            ("a2", &vec![0, 1, 2, 3]),
            ("b2", &vec![3, 4, 5, 6]),
            ("c2", &vec![4, 5, 6, 7]),
        );

        let schema = batch1.schema();
        let env = Arc::new(RuntimeEnv::default());
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let spill_manager = SpillManager::new(env, metrics, Arc::clone(&schema));

        let (spill_file, max_batch_mem) = spill_manager
            .spill_record_batch_by_size_and_return_max_batch_memory(
                &batch1,
                "Test Spill",
                1,
            )?
            .unwrap();
        assert!(spill_file.path().exists());
        assert!(max_batch_mem > 0);

        let stream = spill_manager.read_spill_as_stream(spill_file, None)?;
        assert_eq!(stream.schema(), schema);

        let batches = collect(stream).await?;
        assert_eq!(batches.len(), 4);

        Ok(())
    }

    fn build_compressible_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, true),
        ]));

        let a: ArrayRef = Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
            "repeated", 100,
        )));
        let b: ArrayRef = Arc::new(Int32Array::from(vec![1; 100]));
        let c: ArrayRef = Arc::new(Int32Array::from(vec![2; 100]));

        RecordBatch::try_new(schema, vec![a, b, c]).unwrap()
    }

    async fn validate(
        spill_manager: &SpillManager,
        spill_file: RefCountedTempFile,
        num_rows: usize,
        schema: SchemaRef,
        batch_count: usize,
    ) -> Result<()> {
        let spilled_rows = spill_manager.metrics.spilled_rows.value();
        assert_eq!(spilled_rows, num_rows);

        let stream = spill_manager.read_spill_as_stream(spill_file, None)?;
        assert_eq!(stream.schema(), schema);

        let batches = collect(stream).await?;
        assert_eq!(batches.len(), batch_count);

        Ok(())
    }

    #[tokio::test]
    async fn test_spill_compression() -> Result<()> {
        let batch = build_compressible_batch();
        let num_rows = batch.num_rows();
        let schema = batch.schema();
        let batch_count = 1;
        let batches = [batch];

        // Construct SpillManager
        let env = Arc::new(RuntimeEnv::default());
        let uncompressed_metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let lz4_metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let zstd_metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let uncompressed_spill_manager = SpillManager::new(
            Arc::clone(&env),
            uncompressed_metrics,
            Arc::clone(&schema),
        );
        let lz4_spill_manager =
            SpillManager::new(Arc::clone(&env), lz4_metrics, Arc::clone(&schema))
                .with_compression_type(SpillCompression::Lz4Frame);
        let zstd_spill_manager =
            SpillManager::new(env, zstd_metrics, Arc::clone(&schema))
                .with_compression_type(SpillCompression::Zstd);
        let uncompressed_spill_file = uncompressed_spill_manager
            .spill_record_batch_and_finish(&batches, "Test")?
            .unwrap();
        let lz4_spill_file = lz4_spill_manager
            .spill_record_batch_and_finish(&batches, "Lz4_Test")?
            .unwrap();
        let zstd_spill_file = zstd_spill_manager
            .spill_record_batch_and_finish(&batches, "ZSTD_Test")?
            .unwrap();
        assert!(uncompressed_spill_file.path().exists());
        assert!(lz4_spill_file.path().exists());
        assert!(zstd_spill_file.path().exists());

        let lz4_spill_size = std::fs::metadata(lz4_spill_file.path())?.len();
        let zstd_spill_size = std::fs::metadata(zstd_spill_file.path())?.len();
        let uncompressed_spill_size =
            std::fs::metadata(uncompressed_spill_file.path())?.len();

        assert!(uncompressed_spill_size > lz4_spill_size);
        assert!(uncompressed_spill_size > zstd_spill_size);

        validate(
            &lz4_spill_manager,
            lz4_spill_file,
            num_rows,
            Arc::clone(&schema),
            batch_count,
        )
        .await?;
        validate(
            &zstd_spill_manager,
            zstd_spill_file,
            num_rows,
            Arc::clone(&schema),
            batch_count,
        )
        .await?;
        validate(
            &uncompressed_spill_manager,
            uncompressed_spill_file,
            num_rows,
            schema,
            batch_count,
        )
        .await?;
        Ok(())
    }

    #[test]
    fn test_get_record_batch_memory_size() {
        // Create a simple record batch with two columns
        let schema = Arc::new(Schema::new(vec![
            Field::new("ints", DataType::Int32, true),
            Field::new("float64", DataType::Float64, false),
        ]));

        let int_array =
            Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4), Some(5)]);
        let float64_array = Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]);

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(int_array), Arc::new(float64_array)],
        )
        .unwrap();

        let size = get_record_batch_memory_size(&batch);
        assert_eq!(size, 60);
    }

    #[test]
    fn test_get_record_batch_memory_size_with_null() {
        // Create a simple record batch with two columns
        let schema = Arc::new(Schema::new(vec![
            Field::new("ints", DataType::Int32, true),
            Field::new("float64", DataType::Float64, false),
        ]));

        let int_array = Int32Array::from(vec![None, Some(2), Some(3)]);
        let float64_array = Float64Array::from(vec![1.0, 2.0, 3.0]);

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(int_array), Arc::new(float64_array)],
        )
        .unwrap();

        let size = get_record_batch_memory_size(&batch);
        assert_eq!(size, 100);
    }

    #[test]
    fn test_get_record_batch_memory_size_empty() {
        // Test with empty record batch
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ints",
            DataType::Int32,
            false,
        )]));

        let int_array: Int32Array = Int32Array::from(vec![] as Vec<i32>);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(int_array)]).unwrap();

        let size = get_record_batch_memory_size(&batch);
        assert_eq!(size, 0, "Empty batch should have 0 memory size");
    }

    #[test]
    fn test_get_record_batch_memory_size_shared_buffer() {
        // Test with slices that share the same underlying buffer
        let original = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let slice1 = original.slice(0, 3);
        let slice2 = original.slice(2, 3);

        // `RecordBatch` with `original` array
        // ----
        let schema_origin = Arc::new(Schema::new(vec![Field::new(
            "origin_col",
            DataType::Int32,
            false,
        )]));
        let batch_origin =
            RecordBatch::try_new(schema_origin, vec![Arc::new(original)]).unwrap();

        // `RecordBatch` with all columns are reference to `original` array
        // ----
        let schema = Arc::new(Schema::new(vec![
            Field::new("slice1", DataType::Int32, false),
            Field::new("slice2", DataType::Int32, false),
        ]));

        let batch_sliced =
            RecordBatch::try_new(schema, vec![Arc::new(slice1), Arc::new(slice2)])
                .unwrap();

        // Two sizes should all be only counting the buffer in `original` array
        let size_origin = get_record_batch_memory_size(&batch_origin);
        let size_sliced = get_record_batch_memory_size(&batch_sliced);

        assert_eq!(size_origin, size_sliced);
    }

    #[test]
    fn test_get_record_batch_memory_size_nested_array() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "nested_int",
                DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
                false,
            ),
            Field::new(
                "nested_int2",
                DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
                false,
            ),
        ]));

        let int_list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(3)]),
        ]);

        let int_list_array2 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(4), Some(5), Some(6)]),
        ]);

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(int_list_array), Arc::new(int_list_array2)],
        )
        .unwrap();

        let size = get_record_batch_memory_size(&batch);
        assert_eq!(size, 8208);
    }

    // ==== Spill manager tests ====

    #[test]
    fn test_spill_manager_spill_record_batch_and_finish() -> Result<()> {
        let env = Arc::new(RuntimeEnv::default());
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));

        let spill_manager = SpillManager::new(env, metrics, Arc::clone(&schema));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )?;

        let temp_file = spill_manager.spill_record_batch_and_finish(&[batch], "Test")?;
        assert!(temp_file.is_some());
        assert!(temp_file.unwrap().path().exists());
        Ok(())
    }

    fn verify_metrics(
        in_progress_file: &InProgressSpillFile,
        expected_spill_file_count: usize,
        expected_spilled_bytes: usize,
        expected_spilled_rows: usize,
    ) -> Result<()> {
        let actual_spill_file_count = in_progress_file
            .spill_writer
            .metrics
            .spill_file_count
            .value();
        let actual_spilled_bytes =
            in_progress_file.spill_writer.metrics.spilled_bytes.value();
        let actual_spilled_rows =
            in_progress_file.spill_writer.metrics.spilled_rows.value();

        assert_eq!(
            actual_spill_file_count, expected_spill_file_count,
            "Spill file count mismatch"
        );
        assert_eq!(
            actual_spilled_bytes, expected_spilled_bytes,
            "Spilled bytes mismatch"
        );
        assert_eq!(
            actual_spilled_rows, expected_spilled_rows,
            "Spilled rows mismatch"
        );

        Ok(())
    }

    #[test]
    fn test_in_progress_spill_file_append_and_finish() -> Result<()> {
        let env = Arc::new(RuntimeEnv::default());
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));

        let spill_manager =
            Arc::new(SpillManager::new(env, metrics, Arc::clone(&schema)));
        let mut in_progress_file = spill_manager.create_in_progress_file("Test")?;

        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )?;

        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(StringArray::from(vec!["d", "e", "f"])),
            ],
        )?;
        // After appending each batch, spilled_rows should increase, while spill_file_count and
        // spilled_bytes remain the same (spilled_bytes is updated only after finish() is called)
        in_progress_file.append_batch(&batch1)?;
        verify_metrics(&in_progress_file, 1, 0, 3)?;

        in_progress_file.append_batch(&batch2)?;
        verify_metrics(&in_progress_file, 1, 0, 6)?;

        let completed_file = in_progress_file.finish()?;
        assert!(completed_file.is_some());
        assert!(completed_file.unwrap().path().exists());
        verify_metrics(&in_progress_file, 1, 712, 6)?;
        // Double finish produce error
        let result = in_progress_file.finish();
        assert!(result.is_err());

        Ok(())
    }

    // Test write no batches
    #[test]
    fn test_in_progress_spill_file_write_no_batches() -> Result<()> {
        let env = Arc::new(RuntimeEnv::default());
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));

        let spill_manager =
            Arc::new(SpillManager::new(env, metrics, Arc::clone(&schema)));

        // Test write empty batch with interface `InProgressSpillFile` and `append_batch()`
        let mut in_progress_file = spill_manager.create_in_progress_file("Test")?;
        let completed_file = in_progress_file.finish()?;
        assert!(completed_file.is_none());

        // Test write empty batch with interface `spill_record_batch_and_finish()`
        let completed_file = spill_manager.spill_record_batch_and_finish(&[], "Test")?;
        assert!(completed_file.is_none());

        // Test write empty batch with interface `spill_record_batch_by_size_and_return_max_batch_memory()`
        let empty_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(Vec::<Option<i32>>::new())),
                Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            ],
        )?;
        let completed_file = spill_manager
            .spill_record_batch_by_size_and_return_max_batch_memory(
                &empty_batch,
                "Test",
                1,
            )?;
        assert!(completed_file.is_none());

        Ok(())
    }

    #[test]
    fn test_reading_more_spills_than_tokio_blocking_threads() -> Result<()> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap()
            .block_on(async {
                let batch = build_table_i32(
                    ("a2", &vec![0, 1, 2]),
                    ("b2", &vec![3, 4, 5]),
                    ("c2", &vec![4, 5, 6]),
                );

                let schema = batch.schema();

                // Construct SpillManager
                let env = Arc::new(RuntimeEnv::default());
                let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
                let spill_manager = SpillManager::new(env, metrics, Arc::clone(&schema));
                let batches: [_; 10] = std::array::from_fn(|_| batch.clone());

                let spill_file_1 = spill_manager
                    .spill_record_batch_and_finish(&batches, "Test1")?
                    .unwrap();
                let spill_file_2 = spill_manager
                    .spill_record_batch_and_finish(&batches, "Test2")?
                    .unwrap();

                let mut stream_1 =
                    spill_manager.read_spill_as_stream(spill_file_1, None)?;
                let mut stream_2 =
                    spill_manager.read_spill_as_stream(spill_file_2, None)?;
                stream_1.next().await;
                stream_2.next().await;

                Ok(())
            })
    }

    #[test]
    fn test_alignment_for_schema() -> Result<()> {
        let schema = Schema::new(vec![Field::new("strings", DataType::Utf8View, false)]);
        let alignment = get_max_alignment_for_schema(&schema);
        assert_eq!(alignment, 16);

        let schema = Schema::new(vec![
            Field::new("int32", DataType::Int32, false),
            Field::new("int64", DataType::Int64, false),
        ]);
        let alignment = get_max_alignment_for_schema(&schema);
        assert_eq!(alignment, 8);
        Ok(())
    }
}
