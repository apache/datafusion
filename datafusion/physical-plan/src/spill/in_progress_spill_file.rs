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

use datafusion_common::{Result, internal_datafusion_err};
use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion_common::exec_datafusion_err;
use datafusion_execution::spill_file::SpillFile;

use super::{
    AsyncIPCStreamWriter, IPCStreamEncoder, IPCStreamWriter, spill_manager::SpillManager,
    spill_pieces::SpillPieces,
};

enum InProgressWriter {
    Sync(IPCStreamWriter),
    Async(AsyncIPCStreamWriter),
}

/// Represents an in-progress spill file used for writing `RecordBatch`es to disk, created by `SpillManager`.
/// Caller is able to use this struct to incrementally append in-memory batches to
/// the file, and then finalize the file by calling the `finish` method.
pub struct InProgressSpillFile {
    pub(crate) spill_manager: Arc<SpillManager>,
    /// Lazily initialized writer
    writer: Option<InProgressWriter>,
    /// Lazily initialized in-progress file, it will be moved out when the `finish` method is invoked
    in_progress_file: Option<Arc<dyn SpillFile>>,
    /// Upper bound on the in-memory size of each written batch. See
    /// [`Self::with_max_batch_bytes`].
    max_batch_bytes: Option<usize>,
}

impl InProgressSpillFile {
    pub fn new(
        spill_manager: Arc<SpillManager>,
        in_progress_file: Arc<dyn SpillFile>,
    ) -> Self {
        Self {
            spill_manager,
            in_progress_file: Some(in_progress_file),
            writer: None,
            max_batch_bytes: None,
        }
    }

    /// Bounds the in-memory size of each batch written to this file, or `None` for no
    /// bound.
    ///
    /// A batch is bounded by `batch_size` rows, so with wide rows it can be very large in
    /// bytes. A reader must decode each spilled batch whole, and a merge of spill files
    /// reserves memory for the largest batch of each file (`max_record_batch_memory`).
    /// With a bound, a larger batch is split into row ranges of at most this size (best
    /// effort: one row can be larger), and each range is written as its own IPC message.
    /// Batches within the bound are written as they are. Row order is kept, but one
    /// appended batch can come back as several batches when the file is read.
    ///
    /// The bound is set on the writer and not on the [`SpillManager`], so a reader that
    /// expects one batch per append (such as [`crate::spill::spill_pool`]) cannot get a
    /// bounded file from a shared manager.
    pub(crate) fn with_max_batch_bytes(mut self, max_batch_bytes: Option<usize>) -> Self {
        self.max_batch_bytes = max_batch_bytes;
        self
    }

    /// Appends a `RecordBatch` to the spill file, initializing the writer if necessary.
    ///
    /// Before writing, performs GC on StringView/BinaryView arrays to compact backing
    /// buffers. When a view array is sliced, it still references the original full buffers,
    /// causing massive spill files without GC (see issue #19414: 820MB → 33MB after GC).
    ///
    /// Returns the post-GC sliced memory size of the batch for memory accounting. When the
    /// batch size is bounded and the batch is split (see [`Self::with_max_batch_bytes`]),
    /// this is the size of the largest piece,
    /// which is the largest batch a reader of the file decodes.
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

        let pieces = SpillPieces::new(batch, self.max_batch_bytes)?;

        if self.writer.is_none() {
            // Use the SpillManager's declared schema rather than the batch's schema.
            // Individual batches may have different schemas (e.g., different nullability)
            // when they come from different branches of a UnionExec. The SpillManager's
            // schema represents the canonical schema that all batches should conform to.
            let schema = self.spill_manager.schema();
            if let Some(in_progress_file) = &self.in_progress_file {
                let spill_writer = in_progress_file.open_writer()?;

                self.writer = Some(InProgressWriter::Sync(IPCStreamWriter::new(
                    spill_writer,
                    schema.as_ref(),
                    self.spill_manager.compression,
                )?));

                // Update metrics
                self.spill_manager.metrics.spill_file_count.add(1);
            }
        }
        let mut max_size = 0;
        if let Some(InProgressWriter::Sync(writer)) = &mut self.writer {
            for piece in pieces {
                let (piece, size) = piece?;
                // The writer calculates how many serialized bytes were emitted
                let (spilled_rows, delta_bytes) = writer.write(&piece)?;

                self.spill_manager.metrics.spilled_rows.add(spilled_rows);
                self.spill_manager.metrics.spilled_bytes.add(delta_bytes);
                max_size = max_size.max(size);
            }
        } else if self.writer.is_some() {
            return Err(exec_datafusion_err!(
                "Cannot use synchronous append after asynchronous spill writing has started"
            ));
        }
        Ok(max_size)
    }

    /// Appends a `RecordBatch` using the asynchronous spill writer.
    pub async fn append_batch_async(&mut self, batch: &RecordBatch) -> Result<usize> {
        Ok(self.append_batch_async_with_stats(batch).await?.0)
    }

    /// Like [`Self::append_batch_async`], but returns both the memory size and the row
    /// count of the largest batch written. When the batch is split, both describe the
    /// pieces a reader decodes, not the appended batch.
    pub(crate) async fn append_batch_async_with_stats(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<(usize, usize)> {
        if self.in_progress_file.is_none() {
            return Err(exec_datafusion_err!(
                "Append operation failed: No active in-progress file. The file may have already been finalized."
            ));
        }

        let pieces = SpillPieces::new(batch, self.max_batch_bytes)?;

        if self.writer.is_none() {
            let schema = self.spill_manager.schema();
            if let Some(in_progress_file) = &self.in_progress_file {
                // Validate the IPC schema and compression options before opening
                // a remote upload that would otherwise need to be aborted.
                let encoder = IPCStreamEncoder::new(
                    schema.as_ref(),
                    self.spill_manager.compression,
                )?;
                let spill_writer = in_progress_file.open_async_writer().await?;

                self.writer = Some(InProgressWriter::Async(AsyncIPCStreamWriter::new(
                    spill_writer,
                    encoder,
                )));

                self.spill_manager.metrics.spill_file_count.add(1);
            }
        }

        let (mut max_size, mut max_rows) = (0, 0);
        match &mut self.writer {
            Some(InProgressWriter::Async(writer)) => {
                for piece in pieces {
                    let (piece, size) = piece?;
                    let (spilled_rows, delta_bytes) = writer.write(&piece).await?;

                    self.spill_manager.metrics.spilled_rows.add(spilled_rows);
                    self.spill_manager.metrics.spilled_bytes.add(delta_bytes);
                    max_size = max_size.max(size);
                    max_rows = max_rows.max(piece.num_rows());
                }
            }
            Some(InProgressWriter::Sync(_)) => {
                return Err(exec_datafusion_err!(
                    "Cannot use asynchronous append after synchronous spill writing has started"
                ));
            }
            None => {
                return Err(internal_datafusion_err!(
                    "Asynchronous spill writer was not initialized"
                ));
            }
        }
        Ok((max_size, max_rows))
    }

    pub fn flush(&mut self) -> Result<()> {
        if let Some(InProgressWriter::Sync(writer)) = &mut self.writer {
            writer.flush()?;
        } else if self.writer.is_some() {
            return Err(exec_datafusion_err!(
                "Cannot use synchronous flush for an asynchronous spill writer"
            ));
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
        if matches!(self.writer, Some(InProgressWriter::Async(_))) {
            return Err(exec_datafusion_err!(
                "Cannot use synchronous finish for an asynchronous spill writer"
            ));
        }
        if let Some(InProgressWriter::Sync(mut writer)) = self.writer.take() {
            // Finish the writer and capture any final trailing bytes emitted
            let delta_bytes = writer.finish()?;
            self.spill_manager.metrics.spilled_bytes.add(delta_bytes);
        } else {
            return Ok(None);
        }

        Ok(self.in_progress_file.take())
    }

    /// Finalizes an asynchronous spill write, returning the completed file.
    pub async fn finish_async(&mut self) -> Result<Option<Arc<dyn SpillFile>>> {
        if self.in_progress_file.is_none() && self.writer.is_none() {
            return Err(exec_datafusion_err!(
                "Finish operation failed: file has already been finalized."
            ));
        }
        if matches!(self.writer, Some(InProgressWriter::Sync(_))) {
            return Err(exec_datafusion_err!(
                "Cannot use asynchronous finish for a synchronous spill writer"
            ));
        }
        if let Some(InProgressWriter::Async(writer)) = &mut self.writer {
            let delta_bytes = writer.finish().await?;
            self.spill_manager.metrics.spilled_bytes.add(delta_bytes);
        } else {
            return Ok(None);
        }

        self.writer.take();
        Ok(self.in_progress_file.take())
    }

    /// Aborts an asynchronous spill write and discards its file.
    pub async fn abort_async(&mut self) -> Result<()> {
        if matches!(self.writer, Some(InProgressWriter::Sync(_))) {
            return Err(exec_datafusion_err!(
                "Cannot use asynchronous abort for a synchronous spill writer"
            ));
        }

        let result = if let Some(InProgressWriter::Async(writer)) = &mut self.writer {
            writer.abort().await
        } else {
            Ok(())
        };
        self.writer.take();
        self.in_progress_file.take();
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spill::SPILL_BATCH_MEMORY_MARGIN;
    use arrow::array::{
        Array, ArrayRef, AsArray, BinaryViewArray, DictionaryArray, Int32Array,
        Int64Array, ListBuilder, MapBuilder, StringArray, StringBuilder, StringViewArray,
        StructArray, UInt32Array,
    };
    use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
    use datafusion_common::utils::memory::get_record_batch_memory_size;
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

    const WIDE: usize = 64 * 1024;

    fn wide_value(i: usize) -> String {
        format!("{i}{}", "x".repeat(WIDE))
    }

    fn manager(schema: SchemaRef) -> Result<Arc<SpillManager>> {
        let runtime = Arc::new(RuntimeEnvBuilder::new().build()?);
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        Ok(Arc::new(SpillManager::new(runtime, metrics, schema)))
    }

    /// Writes `batches` through a bounded spill file, reads the file back with the reader's
    /// size check on, and returns what was read and the recorded largest batch.
    async fn round_trip(
        batches: &[RecordBatch],
        max_batch_bytes: usize,
    ) -> Result<(Vec<RecordBatch>, usize)> {
        let manager = manager(batches[0].schema())?;
        let mut file = manager
            .create_in_progress_file("test")?
            .with_max_batch_bytes(Some(max_batch_bytes));
        let mut max_memory = 0;
        for batch in batches {
            max_memory = max_memory.max(file.append_batch(batch)?);
        }
        let file = file.finish()?.unwrap();
        let read = manager
            .read_spill_as_stream(file, Some(max_memory))?
            .try_collect::<Vec<_>>()
            .await?;
        // The reader only logs a batch larger than the recorded size plus its margin, so
        // check it here: the merge reserves memory from the recorded size.
        for batch in &read {
            let size = get_record_batch_memory_size(batch);
            assert!(
                size <= max_memory + SPILL_BATCH_MEMORY_MARGIN,
                "read back a batch of {size} bytes, recorded {max_memory}"
            );
        }
        Ok((read, max_memory))
    }

    fn assert_same_rows(read: &[RecordBatch], written: &[RecordBatch]) {
        let schema = written[0].schema();
        let read = arrow::compute::concat_batches(&schema, read).unwrap();
        let written = arrow::compute::concat_batches(&schema, written).unwrap();
        assert_eq!(read, written);
    }

    /// Slices of one large view array keep the whole parent's data buffers alive. The split
    /// must look at what each slice points at, or every half reports the whole parent and the
    /// batch is written unsplit.
    #[tokio::test]
    async fn view_slices_of_a_large_parent_are_split() -> Result<()> {
        let parent = StringViewArray::from_iter_values((0..256).map(wide_value));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::Utf8View,
            false,
        )]));
        // Two slices that each share the parent's buffers (about 16 MiB).
        let batches: Vec<RecordBatch> = [(0, 128), (128, 128)]
            .into_iter()
            .map(|(offset, len)| {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(parent.slice(offset, len))],
                )
            })
            .collect::<std::result::Result<_, _>>()?;

        let budget = 1024 * 1024;
        let (read, max_memory) = round_trip(&batches, budget).await?;
        assert!(
            max_memory <= budget,
            "largest spilled batch is {max_memory} bytes"
        );
        assert!(
            read.len() >= 16,
            "expected at least 16 pieces, got {}",
            read.len()
        );
        assert_same_rows(&read, &batches);
        Ok(())
    }

    /// Many rows share each long value, so the pieces reference overlapping parts of the
    /// parent's data buffers. The bound and the row order must still hold.
    #[tokio::test]
    async fn repeated_view_values_are_split_and_kept_in_order() -> Result<()> {
        let values = (0..256).map(|i| wide_value(i % 4));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "v",
                DataType::Utf8View,
                false,
            )])),
            vec![Arc::new(StringViewArray::from_iter_values(values))],
        )?;
        let budget = 256 * 1024;
        let (read, max_memory) = round_trip(std::slice::from_ref(&batch), budget).await?;
        assert!(
            max_memory <= budget,
            "largest spilled batch is {max_memory} bytes"
        );
        assert!(read.len() > 1);
        assert_same_rows(&read, &[batch]);
        Ok(())
    }

    /// Each key points at its own wide value, so the dictionary values are most of the batch.
    /// Split pieces drop the values they do not use, so the dictionary is divided between
    /// them, and each piece passes the reader's size check.
    #[tokio::test]
    async fn dictionary_with_large_values_is_divided() -> Result<()> {
        let values = StringArray::from_iter_values((0..128).map(wide_value));
        let keys = Int32Array::from_iter_values(0..128);
        let dictionary = DictionaryArray::try_new(keys, Arc::new(values))?;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "d",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            )])),
            vec![Arc::new(dictionary)],
        )?;
        let budget = 1024 * 1024;
        let (read, max_memory) = round_trip(std::slice::from_ref(&batch), budget).await?;
        assert!(
            max_memory <= budget,
            "largest spilled batch is {max_memory} bytes"
        );
        assert!(
            read.len() >= 8,
            "expected at least 8 pieces, got {}",
            read.len()
        );
        let cast =
            |b: &RecordBatch| arrow::compute::cast(b.column(0), &DataType::Utf8).unwrap();
        let read_values: Vec<ArrayRef> = read.iter().map(cast).collect();
        let read_values = arrow::compute::concat(
            &read_values.iter().map(|a| a.as_ref()).collect::<Vec<_>>(),
        )?;
        assert_eq!(&read_values, &cast(&batch));
        Ok(())
    }

    /// One row larger than the budget cannot be split. It is written whole and recorded, so
    /// the reader's check still passes.
    #[tokio::test]
    async fn single_wide_row_is_written_whole() -> Result<()> {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from_iter_values([wide_value(0)]))],
        )?;
        let (read, max_memory) = round_trip(std::slice::from_ref(&batch), 1024).await?;
        assert!(max_memory > 1024);
        assert_eq!(read, vec![batch]);
        Ok(())
    }

    /// Without a bound, one appended batch is one written batch, as before.
    #[tokio::test]
    async fn unbounded_manager_keeps_batches_whole() -> Result<()> {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "v",
                DataType::Utf8View,
                false,
            )])),
            vec![Arc::new(StringViewArray::from_iter_values(
                (0..64).map(wide_value),
            ))],
        )?;
        let manager = manager(batch.schema())?;
        let file = manager
            .spill_record_batch_and_finish(std::slice::from_ref(&batch), "test")?
            .unwrap();
        let read = manager
            .read_spill_as_stream(file, None)?
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(read.len(), 1);
        assert_same_rows(&read, &[batch]);
        Ok(())
    }

    /// A `List<Utf8>` column: `rows` rows of four quarter-`WIDE` strings each.
    fn wide_list(rows: usize) -> ArrayRef {
        let mut builder = ListBuilder::new(StringBuilder::new());
        for i in 0..rows {
            for _ in 0..4 {
                builder
                    .values()
                    .append_value(format!("{i}{}", "x".repeat(WIDE / 4)));
            }
            builder.append(true);
        }
        Arc::new(builder.finish())
    }

    /// A sliced list keeps its whole child array. The split must measure and copy only the
    /// child range that each piece uses, or both halves report the parent and the batch is
    /// written unsplit.
    #[tokio::test]
    async fn list_column_is_split() -> Result<()> {
        let list = wide_list(256);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "l",
            list.data_type().clone(),
            false,
        )]));
        // A slice of a larger parent, as a sort or aggregate emits.
        let batch = RecordBatch::try_new(schema, vec![list.slice(64, 128)])?;
        let budget = 1024 * 1024;
        let (read, max_memory) = round_trip(std::slice::from_ref(&batch), budget).await?;
        assert!(
            max_memory <= budget,
            "largest spilled batch is {max_memory} bytes"
        );
        assert!(
            read.len() >= 8,
            "expected at least 8 pieces, got {}",
            read.len()
        );
        assert_same_rows(&read, &[batch]);
        Ok(())
    }

    /// A map keeps its whole entries array when it is sliced, like a list.
    #[tokio::test]
    async fn map_column_is_split() -> Result<()> {
        let mut builder =
            MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        for i in 0..256 {
            builder.keys().append_value(format!("k{i}"));
            builder.values().append_value(wide_value(i));
            builder.append(true)?;
        }
        let map: ArrayRef = Arc::new(builder.finish());
        let schema = Arc::new(Schema::new(vec![Field::new(
            "m",
            map.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![map.slice(64, 128)])?;
        let budget = 1024 * 1024;
        let (read, max_memory) = round_trip(std::slice::from_ref(&batch), budget).await?;
        assert!(
            max_memory <= budget,
            "largest spilled batch is {max_memory} bytes"
        );
        assert!(
            read.len() >= 8,
            "expected at least 8 pieces, got {}",
            read.len()
        );
        assert_same_rows(&read, &[batch]);
        Ok(())
    }

    /// A struct narrows its children when it is sliced, but a list inside it still keeps its
    /// whole child array.
    #[tokio::test]
    async fn struct_with_list_child_is_split() -> Result<()> {
        let list = wide_list(256);
        let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..256));
        let fields = Fields::from(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("l", list.data_type().clone(), false),
        ]);
        let structs: ArrayRef =
            Arc::new(StructArray::try_new(fields.clone(), vec![ids, list], None)?);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(fields),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![structs.slice(64, 128)])?;
        let budget = 1024 * 1024;
        let (read, max_memory) = round_trip(std::slice::from_ref(&batch), budget).await?;
        assert!(
            max_memory <= budget,
            "largest spilled batch is {max_memory} bytes"
        );
        assert!(
            read.len() >= 8,
            "expected at least 8 pieces, got {}",
            read.len()
        );
        assert_same_rows(&read, &[batch]);
        Ok(())
    }

    fn dictionary_type(values: DataType) -> DataType {
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(values))
    }

    /// A small batch whose keys use a few values of a large dictionary, as `take` produces
    /// when a sort emits a chunk. It is within the budget, so it is written as it is: the
    /// batches of a spill file keep sharing one dictionary, which the reader decodes once.
    #[tokio::test]
    async fn unsplit_batch_keeps_its_dictionary() -> Result<()> {
        let values = StringArray::from_iter_values((0..4).map(|i| format!("v{i}")));
        let keys = Int32Array::from_iter_values([3, 1, 1, 0]);
        let dictionary = DictionaryArray::try_new(keys, Arc::new(values))?;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "d",
                dictionary_type(DataType::Utf8),
                false,
            )])),
            vec![Arc::new(dictionary)],
        )?;
        let pieces =
            SpillPieces::new(&batch, Some(1024 * 1024))?.collect::<Result<Vec<_>>>()?;
        assert_eq!(pieces.len(), 1);
        assert_eq!(pieces[0].0, batch);
        Ok(())
    }

    /// Values of a dictionary inside a struct are not pruned, so every piece would carry
    /// the whole dictionary. The batch is written whole instead.
    #[tokio::test]
    async fn nested_dictionary_is_written_whole() -> Result<()> {
        let values = StringArray::from_iter_values((0..128).map(wide_value));
        let keys = Int32Array::from_iter_values(0..128);
        let dictionary: ArrayRef =
            Arc::new(DictionaryArray::try_new(keys, Arc::new(values))?);
        let fields = Fields::from(vec![Field::new(
            "d",
            dictionary_type(DataType::Utf8),
            false,
        )]);
        let structs: ArrayRef = Arc::new(StructArray::try_new(
            fields.clone(),
            vec![dictionary],
            None,
        )?);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "s",
                DataType::Struct(fields),
                false,
            )])),
            vec![structs],
        )?;
        let budget = 1024 * 1024;
        let (read, _) = round_trip(std::slice::from_ref(&batch), budget).await?;
        assert_eq!(read.len(), 1);
        assert_same_rows(&read, &[batch]);
        Ok(())
    }

    /// Rows of a list view can share child ranges, so copying each piece's ranges out can
    /// multiply the data. A batch with a list view is written whole.
    #[tokio::test]
    async fn list_view_with_shared_child_is_written_whole() -> Result<()> {
        let child: ArrayRef = Arc::new(StringArray::from_iter_values([wide_value(0)]));
        let field = Arc::new(Field::new_list_field(DataType::Utf8, false));
        let rows = 1024;
        let list_view: ArrayRef = Arc::new(arrow::array::ListViewArray::try_new(
            Arc::clone(&field),
            vec![0; rows].into(),
            vec![1; rows].into(),
            child,
            None,
        )?);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "l",
            DataType::ListView(field),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![list_view])?;
        let pieces =
            SpillPieces::new(&batch, Some(16 * 1024))?.collect::<Result<Vec<_>>>()?;
        assert_eq!(pieces.len(), 1);
        assert_eq!(pieces[0].0, batch);
        Ok(())
    }

    /// The largest row count returned with the size describes the pieces, not the
    /// appended batch.
    #[tokio::test]
    async fn stats_count_rows_per_piece() -> Result<()> {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from_iter_values(
                (0..64).map(wide_value),
            ))],
        )?;
        let manager = manager(batch.schema())?;
        let mut file = manager
            .create_in_progress_file("test")?
            .with_max_batch_bytes(Some(4 * WIDE + WIDE / 2));
        let (size, rows) = file.append_batch_async_with_stats(&batch).await?;
        assert!(size <= 4 * WIDE + WIDE / 2);
        assert_eq!(rows, 4);
        Ok(())
    }

    /// A piece whose dictionary keys are all null has no used values. After the unused
    /// values are dropped the dictionary is empty, which must not panic when measured, and
    /// must read back as nulls.
    #[tokio::test]
    async fn dictionary_with_only_null_keys() -> Result<()> {
        let values = StringArray::from_iter_values((0..64).map(wide_value));
        let keys = Int32Array::from_iter((0..64).map(|i| (i >= 8).then_some(i)));
        let dictionary: ArrayRef =
            Arc::new(DictionaryArray::try_new(keys, Arc::new(values))?);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "d",
            dictionary_type(DataType::Utf8),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![dictionary])?;
        // Pieces of at most 4 rows: the first ones have only null keys.
        let budget = 4 * WIDE + WIDE / 2;
        let pieces =
            SpillPieces::new(&batch, Some(budget))?.collect::<Result<Vec<_>>>()?;
        assert!(pieces.len() > 1);
        assert!(
            pieces.iter().any(|(piece, _)| piece
                .column(0)
                .as_any_dictionary()
                .values()
                .is_empty()),
            "expected a piece with an empty dictionary"
        );
        SpillPieces::new(&batch.slice(0, 0), Some(budget))?
            .collect::<Result<Vec<_>>>()?;

        let (read, _) = round_trip(std::slice::from_ref(&batch), budget).await?;
        let cast =
            |b: &RecordBatch| arrow::compute::cast(b.column(0), &DataType::Utf8).unwrap();
        let read_values: Vec<ArrayRef> = read.iter().map(cast).collect();
        let read_values = arrow::compute::concat(
            &read_values.iter().map(|a| a.as_ref()).collect::<Vec<_>>(),
        )?;
        assert_eq!(&read_values, &cast(&batch));
        Ok(())
    }
    fn view_schema(fields: &[(&str, DataType)]) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .iter()
                .map(|(name, t)| Field::new(*name, t.clone(), true))
                .collect::<Vec<_>>(),
        ))
    }

    fn assert_bounded(read: &[RecordBatch], max_memory: usize, budget: usize) {
        assert!(
            max_memory <= budget,
            "largest spilled batch is {max_memory} bytes"
        );
        assert!(read.len() > 1, "expected the batch to be split");
    }

    /// `take` of every 16th row of a large view array keeps all the parent's buffers, but
    /// is not a slice. Garbage collection must see the few values it uses, so the batch
    /// fits the budget and is written as one small piece.
    #[tokio::test]
    async fn sparse_taken_view_array_is_compacted() -> Result<()> {
        let parent = StringViewArray::from_iter_values((0..256).map(wide_value));
        let indices = UInt32Array::from_iter_values((0..256).step_by(16));
        let taken = arrow::compute::take(&parent, &indices, None)?;
        let batch =
            RecordBatch::try_new(view_schema(&[("v", DataType::Utf8View)]), vec![taken])?;
        let budget = 2 * 1024 * 1024;
        assert!(get_record_batch_memory_size(&batch) > budget);
        let (read, max_memory) = round_trip(std::slice::from_ref(&batch), budget).await?;
        assert_eq!(read.len(), 1);
        assert!(max_memory <= budget, "spilled batch is {max_memory} bytes");
        assert_same_rows(&read, &[batch]);
        Ok(())
    }

    /// Sparse `take` outputs of one parent in several batches, each larger than the
    /// budget once compacted: every piece holds only its own values.
    #[tokio::test]
    async fn sparse_taken_views_across_batches_are_split() -> Result<()> {
        let parent = StringViewArray::from_iter_values((0..512).map(wide_value));
        let schema = view_schema(&[("v", DataType::Utf8View)]);
        let batches = [0u32, 1]
            .into_iter()
            .map(|start| {
                let indices = UInt32Array::from_iter_values((start..512).step_by(2));
                let taken = arrow::compute::take(&parent, &indices, None)?;
                Ok(RecordBatch::try_new(Arc::clone(&schema), vec![taken])?)
            })
            .collect::<Result<Vec<_>>>()?;
        let budget = 1024 * 1024;
        let (read, max_memory) = round_trip(&batches, budget).await?;
        assert_bounded(&read, max_memory, budget);
        assert!(read.len() >= 32, "got {} pieces", read.len());
        assert_same_rows(&read, &batches);
        Ok(())
    }

    /// Two columns of one batch share the same view buffers: one is the parent, the other
    /// a reversed `take` of it. Each piece must count and copy each column's own values.
    #[tokio::test]
    async fn columns_sharing_view_buffers_are_split() -> Result<()> {
        let parent: ArrayRef =
            Arc::new(StringViewArray::from_iter_values((0..128).map(wide_value)));
        let reversed = arrow::compute::take(
            &parent,
            &UInt32Array::from_iter_values((0..128).rev()),
            None,
        )?;
        let batch = RecordBatch::try_new(
            view_schema(&[("a", DataType::Utf8View), ("b", DataType::Utf8View)]),
            vec![Arc::clone(&parent), reversed],
        )?;
        let budget = 1024 * 1024;
        let (read, max_memory) = round_trip(std::slice::from_ref(&batch), budget).await?;
        assert_bounded(&read, max_memory, budget);
        assert_same_rows(&read, &[batch]);
        Ok(())
    }

    /// An unsliced view array mixing inline (at most 12 bytes) values, long values and
    /// nulls is split, and every kind of value reads back in order.
    #[tokio::test]
    async fn mixed_inline_long_and_null_views_are_split() -> Result<()> {
        let values = (0..256).map(|i| match i % 3 {
            0 => None,
            1 => Some(format!("s{i}")),
            _ => Some(wide_value(i)),
        });
        let batch = RecordBatch::try_new(
            view_schema(&[("v", DataType::Utf8View)]),
            vec![Arc::new(StringViewArray::from_iter(values))],
        )?;
        let budget = 1024 * 1024;
        let (read, max_memory) = round_trip(std::slice::from_ref(&batch), budget).await?;
        assert_bounded(&read, max_memory, budget);
        assert_same_rows(&read, &[batch]);
        Ok(())
    }

    /// Binary views are split like string views.
    #[tokio::test]
    async fn binary_view_column_is_split() -> Result<()> {
        let values: Vec<Vec<u8>> = (0..128).map(|i| wide_value(i).into_bytes()).collect();
        let batch = RecordBatch::try_new(
            view_schema(&[("b", DataType::BinaryView)]),
            vec![Arc::new(BinaryViewArray::from_iter_values(values))],
        )?;
        let budget = 1024 * 1024;
        let (read, max_memory) = round_trip(std::slice::from_ref(&batch), budget).await?;
        assert_bounded(&read, max_memory, budget);
        assert_same_rows(&read, &[batch]);
        Ok(())
    }

    /// A dictionary whose values are views: pruning the dictionary must also drop the
    /// view buffers that only the unused values pointed at.
    #[tokio::test]
    async fn dictionary_of_views_is_divided() -> Result<()> {
        let values = StringViewArray::from_iter_values((0..128).map(wide_value));
        let keys = Int32Array::from_iter_values(0..128);
        let dictionary = DictionaryArray::try_new(keys, Arc::new(values))?;
        let batch = RecordBatch::try_new(
            view_schema(&[("d", dictionary_type(DataType::Utf8View))]),
            vec![Arc::new(dictionary)],
        )?;
        let budget = 1024 * 1024;
        let (read, max_memory) = round_trip(std::slice::from_ref(&batch), budget).await?;
        assert_bounded(&read, max_memory, budget);
        let cast =
            |b: &RecordBatch| arrow::compute::cast(b.column(0), &DataType::Utf8).unwrap();
        let read_values: Vec<ArrayRef> = read.iter().map(cast).collect();
        let read_values = arrow::compute::concat(
            &read_values.iter().map(|a| a.as_ref()).collect::<Vec<_>>(),
        )?;
        assert_eq!(&read_values, &cast(&batch));
        Ok(())
    }

    /// Views inside a struct and a list are below the top level. Each piece must still
    /// hold only the view data of its own rows.
    #[tokio::test]
    async fn nested_views_are_split() -> Result<()> {
        let views: ArrayRef =
            Arc::new(StringViewArray::from_iter_values((0..256).map(wide_value)));
        let fields = Fields::from(vec![Field::new("v", DataType::Utf8View, false)]);
        let structs: ArrayRef =
            Arc::new(StructArray::try_new(fields.clone(), vec![views], None)?);
        let item = Arc::new(Field::new_list_field(DataType::Utf8View, false));
        let lists: ArrayRef = Arc::new(arrow::array::ListArray::try_new(
            Arc::clone(&item),
            arrow::buffer::OffsetBuffer::from_lengths(vec![1; 256]),
            Arc::new(StringViewArray::from_iter_values((0..256).map(wide_value))),
            None,
        )?);
        let batch = RecordBatch::try_new(
            view_schema(&[("s", DataType::Struct(fields)), ("l", DataType::List(item))]),
            vec![structs, lists],
        )?;
        let budget = 2 * 1024 * 1024;
        let (read, max_memory) = round_trip(std::slice::from_ref(&batch), budget).await?;
        assert_bounded(&read, max_memory, budget);
        assert_same_rows(&read, &[batch]);
        Ok(())
    }
}
