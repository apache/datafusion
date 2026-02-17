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

// Moved for refactor, re-export to keep the public API stable
pub use datafusion_common::utils::memory::get_record_batch_memory_size;
// Re-export SpillManager for doctests only (hidden from public docs)
#[doc(hidden)]
pub use spill_manager::SpillManager;

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{
    Array, BinaryViewArray, BufferSpec, GenericByteViewArray, StringViewArray, layout,
};
use arrow::datatypes::{ByteViewType, Schema, SchemaRef};
use arrow::ipc::{
    MetadataVersion,
    reader::StreamReader,
    writer::{IpcWriteOptions, StreamWriter},
};
use arrow::record_batch::RecordBatch;

use datafusion_common::config::SpillCompression;
use datafusion_common::{DataFusionError, Result, exec_datafusion_err};
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::RecordBatchStream;
use datafusion_execution::disk_manager::RefCountedTempFile;
use futures::{FutureExt as _, Stream};
use log::debug;

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
                                        debug!(
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

    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
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

/// Size of a single view structure in StringView/BinaryView arrays (in bytes).
/// Each view is 16 bytes: 4 bytes length + 4 bytes prefix + 8 bytes buffer ID/offset.
#[cfg(test)]
const VIEW_SIZE_BYTES: usize = 16;

/// Performs garbage collection on StringView and BinaryView arrays before spilling to reduce memory usage.
///
/// # Why GC is needed
///
/// StringView and BinaryView arrays can accumulate significant memory waste when sliced.
/// When a large array is sliced (e.g., taking first 100 rows of 1000), the view array
/// still references the original data buffers containing all 1000 rows of data.
///
/// For example, in the ClickBench benchmark (issue #19414), repeated slicing of StringView
/// arrays resulted in 820MB of spill files that could be reduced to just 33MB after GC -
/// a 96% reduction in size.
///
/// # How it works
///
/// The GC process:
/// 1. Identifies view arrays (StringView/BinaryView) in the batch
/// 2. Checks if their data buffers exceed a memory threshold
/// 3. If exceeded, calls the Arrow `gc()` method which creates new compact buffers
///    containing only the data referenced by the current views
/// 4. Returns a new batch with GC'd arrays (or original arrays if GC not needed)
///
/// # When GC is triggered
///
/// GC is only performed when data buffers exceed a threshold (currently 10KB).
/// This balances memory savings against the CPU overhead of garbage collection.
/// Small arrays are passed through unchanged since the GC overhead would exceed
/// any memory savings.
///
/// # Performance considerations
///
/// The function always returns a new RecordBatch for API consistency, but:
/// - If no view arrays are present, it's a cheap clone (just Arc increments)
/// - GC is skipped for small buffers to avoid unnecessary CPU overhead
/// - The Arrow `gc()` method itself is optimized and only copies referenced data
pub(crate) fn gc_view_arrays(batch: &RecordBatch) -> Result<RecordBatch> {
    // Early return optimization: Skip GC entirely if the batch contains no view arrays.
    // This avoids unnecessary processing for batches with only primitive types.
    let has_view_arrays = batch.columns().iter().any(|array| {
        matches!(
            array.data_type(),
            arrow::datatypes::DataType::Utf8View | arrow::datatypes::DataType::BinaryView
        )
    });

    if !has_view_arrays {
        // RecordBatch::clone() is cheap - just Arc reference count bumps
        return Ok(batch.clone());
    }

    let mut new_columns: Vec<Arc<dyn Array>> = Vec::with_capacity(batch.num_columns());

    for array in batch.columns() {
        let gc_array = match array.data_type() {
            arrow::datatypes::DataType::Utf8View => {
                let string_view = array
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .expect("Utf8View array should downcast to StringViewArray");
                // Only perform GC if the array appears to be sliced (has potential waste).
                // The gc() method internally checks if GC is beneficial.
                if should_gc_view_array(string_view) {
                    Arc::new(string_view.gc()) as Arc<dyn Array>
                } else {
                    Arc::clone(array)
                }
            }
            arrow::datatypes::DataType::BinaryView => {
                let binary_view = array
                    .as_any()
                    .downcast_ref::<BinaryViewArray>()
                    .expect("BinaryView array should downcast to BinaryViewArray");
                // Only perform GC if the array appears to be sliced (has potential waste).
                // The gc() method internally checks if GC is beneficial.
                if should_gc_view_array(binary_view) {
                    Arc::new(binary_view.gc()) as Arc<dyn Array>
                } else {
                    Arc::clone(array)
                }
            }
            // Non-view arrays are passed through unchanged
            _ => Arc::clone(array),
        };
        new_columns.push(gc_array);
    }

    // Always return a new batch for consistency
    Ok(RecordBatch::try_new(batch.schema(), new_columns)?)
}

/// Determines whether a view array should be garbage collected.
///
/// This function checks if:
/// 1. The array has data buffers (non-inline strings/binaries)
/// 2. The total buffer memory exceeds a threshold
/// 3. The array appears to be sliced (has potential memory waste)
///
/// The Arrow `gc()` method itself is a no-op for arrays that don't benefit from GC,
/// but we still check here to avoid the overhead of calling gc() unnecessarily.
///
/// # Memory threshold rationale
///
/// We use a 10KB threshold based on:
/// - Small arrays (< 10KB) have negligible memory impact even if sliced
/// - GC has CPU overhead that isn't worth it for small arrays
/// - This threshold captures pathological cases (e.g., ClickBench: 820MB -> 33MB)
///   while avoiding unnecessary GC on small working sets
fn should_gc_view_array<T: ByteViewType>(array: &GenericByteViewArray<T>) -> bool {
    const MIN_BUFFER_SIZE_FOR_GC: usize = 10 * 1024; // 10KB threshold

    let data_buffers = array.data_buffers();
    if data_buffers.is_empty() {
        // All strings/binaries are inlined (< 12 bytes), no GC needed
        return false;
    }

    // Calculate total buffer memory
    let total_buffer_size: usize = data_buffers.iter().map(|b| b.capacity()).sum();

    // Only GC if buffers exceed threshold
    total_buffer_size > MIN_BUFFER_SIZE_FOR_GC
}

#[cfg(test)]
fn calculate_string_view_waste_ratio(array: &StringViewArray) -> f64 {
    use arrow_data::MAX_INLINE_VIEW_LEN;
    calculate_view_waste_ratio(array.len(), array.data_buffers(), |i| {
        if !array.is_null(i) {
            let value = array.value(i);
            if value.len() > MAX_INLINE_VIEW_LEN as usize {
                return value.len();
            }
        }
        0
    })
}

#[cfg(test)]
fn calculate_view_waste_ratio<F>(
    len: usize,
    data_buffers: &[arrow::buffer::Buffer],
    get_value_size: F,
) -> f64
where
    F: Fn(usize) -> usize,
{
    let total_buffer_size: usize = data_buffers.iter().map(|b| b.capacity()).sum();
    if total_buffer_size == 0 {
        return 0.0;
    }

    let mut actual_used_size = (0..len).map(get_value_size).sum::<usize>();
    actual_used_size += len * VIEW_SIZE_BYTES;

    let waste = total_buffer_size.saturating_sub(actual_used_size);
    waste as f64 / total_buffer_size as f64
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
    use arrow::array::{ArrayRef, Int32Array, StringArray};
    use arrow::compute::cast;
    use arrow::datatypes::{DataType, Field, Schema};
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
        // After appending each batch, spilled_rows and spilled_bytes should increase incrementally,
        // while spill_file_count remains 1 (since we're writing to the same file)
        in_progress_file.append_batch(&batch1)?;
        verify_metrics(&in_progress_file, 1, 440, 3)?;

        in_progress_file.append_batch(&batch2)?;
        verify_metrics(&in_progress_file, 1, 704, 6)?;

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
    #[tokio::test]
    async fn test_real_time_spill_metrics() -> Result<()> {
        let env = Arc::new(RuntimeEnv::default());
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));

        let spill_manager = Arc::new(SpillManager::new(
            Arc::clone(&env),
            metrics.clone(),
            Arc::clone(&schema),
        ));
        let mut in_progress_file = spill_manager.create_in_progress_file("Test")?;

        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )?;

        // Before any batch, metrics should be 0
        assert_eq!(metrics.spilled_bytes.value(), 0);
        assert_eq!(metrics.spill_file_count.value(), 0);

        // Append first batch
        in_progress_file.append_batch(&batch1)?;

        // Metrics should be updated immediately (at least schema and first batch)
        let bytes_after_batch1 = metrics.spilled_bytes.value();
        assert_eq!(bytes_after_batch1, 440);
        assert_eq!(metrics.spill_file_count.value(), 1);

        // Check global progress
        let progress = env.spilling_progress();
        assert_eq!(progress.current_bytes, bytes_after_batch1 as u64);
        assert_eq!(progress.active_files_count, 1);

        // Append another batch
        in_progress_file.append_batch(&batch1)?;
        let bytes_after_batch2 = metrics.spilled_bytes.value();
        assert!(bytes_after_batch2 > bytes_after_batch1);

        // Check global progress again
        let progress = env.spilling_progress();
        assert_eq!(progress.current_bytes, bytes_after_batch2 as u64);

        // Finish the file
        let spilled_file = in_progress_file.finish()?;
        let final_bytes = metrics.spilled_bytes.value();
        assert!(final_bytes > bytes_after_batch2);

        // Even after finish, file is still "active" until dropped
        let progress = env.spilling_progress();
        assert!(progress.current_bytes > 0);
        assert_eq!(progress.active_files_count, 1);

        drop(spilled_file);
        assert_eq!(env.spilling_progress().active_files_count, 0);
        assert_eq!(env.spilling_progress().current_bytes, 0);

        Ok(())
    }

    #[test]
    fn test_gc_string_view_before_spill() -> Result<()> {
        use arrow::array::StringViewArray;

        let strings: Vec<String> = (0..1000)
            .map(|i| {
                if i % 2 == 0 {
                    "short_string".to_string()
                } else {
                    "this_is_a_much_longer_string_that_will_not_be_inlined".to_string()
                }
            })
            .collect();

        let string_array = StringViewArray::from(strings);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "strings",
            DataType::Utf8View,
            false,
        )]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(string_array) as ArrayRef],
        )?;
        let sliced_batch = batch.slice(0, 100);
        let gc_batch = gc_view_arrays(&sliced_batch)?;

        assert_eq!(gc_batch.num_rows(), sliced_batch.num_rows());
        assert_eq!(gc_batch.num_columns(), sliced_batch.num_columns());

        Ok(())
    }

    #[test]
    fn test_gc_binary_view_before_spill() -> Result<()> {
        use arrow::array::BinaryViewArray;

        let binaries: Vec<Vec<u8>> = (0..1000)
            .map(|i| {
                if i % 2 == 0 {
                    vec![1, 2, 3, 4]
                } else {
                    vec![1; 50]
                }
            })
            .collect();

        let binary_array =
            BinaryViewArray::from_iter(binaries.iter().map(|b| Some(b.as_slice())));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "binaries",
            DataType::BinaryView,
            false,
        )]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(binary_array) as ArrayRef],
        )?;
        let sliced_batch = batch.slice(0, 100);
        let gc_batch = gc_view_arrays(&sliced_batch)?;

        assert_eq!(gc_batch.num_rows(), sliced_batch.num_rows());
        assert_eq!(gc_batch.num_columns(), sliced_batch.num_columns());

        Ok(())
    }

    #[test]
    fn test_gc_skips_small_arrays() -> Result<()> {
        use arrow::array::StringViewArray;

        let strings: Vec<String> = (0..10).map(|i| format!("string_{i}")).collect();

        let string_array = StringViewArray::from(strings);
        let array_ref: ArrayRef = Arc::new(string_array);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "strings",
            DataType::Utf8View,
            false,
        )]));

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array_ref])?;

        // GC should return the original batch for small arrays
        let gc_batch = gc_view_arrays(&batch)?;

        // The batch should be unchanged (cloned, not GC'd)
        assert_eq!(gc_batch.num_rows(), batch.num_rows());

        Ok(())
    }

    #[test]
    fn test_gc_with_mixed_columns() -> Result<()> {
        use arrow::array::{Int32Array, StringViewArray};

        let strings: Vec<String> = (0..200)
            .map(|i| format!("long_string_for_gc_testing_{i}"))
            .collect();

        let string_array = StringViewArray::from(strings);
        let int_array = Int32Array::from((0..200).collect::<Vec<i32>>());

        let schema = Arc::new(Schema::new(vec![
            Field::new("strings", DataType::Utf8View, false),
            Field::new("ints", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(string_array) as ArrayRef,
                Arc::new(int_array) as ArrayRef,
            ],
        )?;

        let sliced_batch = batch.slice(0, 50);
        let gc_batch = gc_view_arrays(&sliced_batch)?;

        assert_eq!(gc_batch.num_columns(), 2);
        assert_eq!(gc_batch.num_rows(), 50);

        Ok(())
    }

    #[test]
    fn test_verify_gc_triggers_for_sliced_arrays() -> Result<()> {
        let strings: Vec<String> = (0..1000)
            .map(|i| {
                format!(
                    "http://example.com/very/long/path/that/exceeds/inline/threshold/{i}"
                )
            })
            .collect();

        let string_array = StringViewArray::from(strings);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "url",
            DataType::Utf8View,
            false,
        )]));

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(string_array.clone()) as ArrayRef],
        )?;

        let sliced = batch.slice(0, 100);

        let sliced_array = sliced
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        let should_gc = should_gc_view_array(sliced_array);
        let waste_ratio = calculate_string_view_waste_ratio(sliced_array);

        assert!(
            waste_ratio > 0.8,
            "Waste ratio should be > 0.8 for sliced array"
        );
        assert!(
            should_gc,
            "GC should trigger for sliced array with high waste"
        );

        Ok(())
    }

    #[test]
    fn test_reproduce_issue_19414_string_view_spill_without_gc() -> Result<()> {
        use arrow::array::StringViewArray;
        use std::fs;

        let num_rows = 5000;
        let mut strings = Vec::with_capacity(num_rows);

        for i in 0..num_rows {
            let url = match i % 5 {
                0 => format!(
                    "http://irr.ru/index.php?showalbum/login-leniya7777294,938303130/{i}"
                ),
                1 => format!("http://komme%2F27.0.1453.116/very/long/path/{i}"),
                2 => format!("https://produkty%2Fproduct/category/item/{i}"),
                3 => format!(
                    "http://irr.ru/index.php?showalbum/login-kapusta-advert2668/{i}"
                ),
                4 => format!(
                    "http://irr.ru/index.php?showalbum/login-kapustic/product/{i}"
                ),
                _ => unreachable!(),
            };
            strings.push(url);
        }

        let string_array = StringViewArray::from(strings);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "URL",
            DataType::Utf8View,
            false,
        )]));

        let original_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(string_array.clone()) as ArrayRef],
        )?;

        let total_buffer_size: usize = string_array
            .data_buffers()
            .iter()
            .map(|buffer| buffer.capacity())
            .sum();

        let mut sliced_batches = Vec::new();
        let slice_size = 100;

        for i in (0..num_rows).step_by(slice_size) {
            let len = std::cmp::min(slice_size, num_rows - i);
            let sliced = original_batch.slice(i, len);
            sliced_batches.push(sliced);
        }

        let env = Arc::new(RuntimeEnv::default());
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let spill_manager = SpillManager::new(env, metrics, schema);

        let mut in_progress_file = spill_manager.create_in_progress_file("Test GC")?;

        for batch in &sliced_batches {
            in_progress_file.append_batch(batch)?;
        }

        let spill_file = in_progress_file.finish()?.unwrap();
        let file_size = fs::metadata(spill_file.path())?.len() as usize;

        let theoretical_without_gc = total_buffer_size * sliced_batches.len();
        let reduction_percent = ((theoretical_without_gc - file_size) as f64
            / theoretical_without_gc as f64)
            * 100.0;

        assert!(
            reduction_percent > 80.0,
            "GC should reduce spill file size by >80%, got {reduction_percent:.1}%"
        );

        Ok(())
    }

    #[test]
    fn test_spill_with_and_without_gc_comparison() -> Result<()> {
        let num_rows = 2000;
        let strings: Vec<String> = (0..num_rows)
            .map(|i| {
                format!(
                    "http://example.com/this/is/a/long/url/path/that/wont/be/inlined/{i}"
                )
            })
            .collect();

        let string_array = StringViewArray::from(strings);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "url",
            DataType::Utf8View,
            false,
        )]));

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(string_array) as ArrayRef])?;

        let sliced_batch = batch.slice(0, 200);

        let array_without_gc = sliced_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        let size_without_gc: usize = array_without_gc
            .data_buffers()
            .iter()
            .map(|buffer| buffer.capacity())
            .sum();

        let gc_batch = gc_view_arrays(&sliced_batch)?;
        let array_with_gc = gc_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        let size_with_gc: usize = array_with_gc
            .data_buffers()
            .iter()
            .map(|buffer| buffer.capacity())
            .sum();

        let reduction_percent =
            ((size_without_gc - size_with_gc) as f64 / size_without_gc as f64) * 100.0;

        assert!(
            reduction_percent > 85.0,
            "Expected >85% reduction for 10% slice, got {reduction_percent:.1}%"
        );

        Ok(())
    }
}
