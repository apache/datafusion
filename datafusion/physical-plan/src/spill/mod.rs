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

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::ptr::NonNull;

use arrow::array::ArrayData;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::ipc::{reader::StreamReader, writer::StreamWriter};
use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc::Sender;

use datafusion_common::{exec_datafusion_err, HashSet, Result};

fn read_spill(sender: Sender<Result<RecordBatch>>, path: &Path) -> Result<()> {
    let file = BufReader::new(File::open(path)?);
    let reader = StreamReader::try_new(file, None)?;
    for batch in reader {
        sender
            .blocking_send(batch.map_err(Into::into))
            .map_err(|e| exec_datafusion_err!("{e}"))?;
    }
    Ok(())
}

/// Spill the `RecordBatch` to disk as smaller batches
/// split by `batch_size_rows`
#[deprecated(
    since = "46.0.0",
    note = "This method is deprecated. Use `SpillManager::spill_record_batch_by_size` instead."
)]
pub fn spill_record_batch_by_size(
    batch: &RecordBatch,
    path: PathBuf,
    schema: SchemaRef,
    batch_size_rows: usize,
) -> Result<()> {
    let mut offset = 0;
    let total_rows = batch.num_rows();
    let mut writer = IPCStreamWriter::new(&path, schema.as_ref())?;

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

    if let Some(null_buffer) = array_data.nulls() {
        if counted_buffers.insert(null_buffer.inner().inner().data_ptr()) {
            *total_size += null_buffer.inner().inner().capacity();
        }
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
    pub fn new(path: &Path, schema: &Schema) -> Result<Self> {
        let file = File::create(path).map_err(|e| {
            exec_datafusion_err!("Failed to create partition file at {path:?}: {e:?}")
        })?;
        Ok(Self {
            num_batches: 0,
            num_rows: 0,
            num_bytes: 0,
            writer: StreamWriter::try_new(file, schema)?,
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

#[cfg(test)]
mod tests {
    use super::in_progress_spill_file::InProgressSpillFile;
    use super::*;
    use crate::common::collect;
    use crate::metrics::ExecutionPlanMetricsSet;
    use crate::metrics::SpillMetrics;
    use crate::spill::spill_manager::SpillManager;
    use crate::test::build_table_i32;
    use arrow::array::{Float64Array, Int32Array, ListArray, StringArray};
    use arrow::compute::cast;
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion_common::Result;
    use datafusion_execution::runtime_env::RuntimeEnv;

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

        let stream = spill_manager.read_spill_as_stream(spill_file)?;
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

        let stream = spill_manager.read_spill_as_stream(spill_file)?;
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

        let spill_file = spill_manager
            .spill_record_batch_by_size(&batch1, "Test Spill", 1)?
            .unwrap();
        assert!(spill_file.path().exists());

        let stream = spill_manager.read_spill_as_stream(spill_file)?;
        assert_eq!(stream.schema(), schema);

        let batches = collect(stream).await?;
        assert_eq!(batches.len(), 4);

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
        assert_eq!(size, 8320);
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

        in_progress_file.append_batch(&batch1)?;
        verify_metrics(&in_progress_file, 1, 356, 3)?;

        in_progress_file.append_batch(&batch2)?;
        verify_metrics(&in_progress_file, 1, 712, 6)?;

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

        // Test write empty batch with interface `spill_record_batch_by_size()`
        let empty_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(Vec::<Option<i32>>::new())),
                Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            ],
        )?;
        let completed_file =
            spill_manager.spill_record_batch_by_size(&empty_batch, "Test", 1)?;
        assert!(completed_file.is_none());

        Ok(())
    }
}
