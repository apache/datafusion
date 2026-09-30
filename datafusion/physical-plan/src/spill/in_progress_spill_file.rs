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

use arrow::array::{Array, ArrayRef, AsArray, GenericByteViewArray, RecordBatch};
use arrow::datatypes::{ByteViewType, DataType};
use arrow_data::MAX_INLINE_VIEW_LEN;
use arrow_select::dictionary::garbage_collect_any_dictionary;
use datafusion_common::exec_datafusion_err;
use datafusion_execution::spill_file::SpillFile;

use super::{
    AsyncIPCStreamWriter, IPCStreamEncoder, IPCStreamWriter, gc_view_arrays,
    spill_manager::{GetSlicedSize, SpillManager},
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
        }
    }

    /// Appends a `RecordBatch` to the spill file, initializing the writer if necessary.
    ///
    /// Before writing, performs GC on StringView/BinaryView arrays to compact backing
    /// buffers. When a view array is sliced, it still references the original full buffers,
    /// causing massive spill files without GC (see issue #19414: 820MB → 33MB after GC).
    ///
    /// Returns the post-GC sliced memory size of the batch for memory accounting. When the
    /// [`SpillManager`] bounds the batch size and the batch is split, this is the size of the
    /// largest piece, which is the largest batch a reader of the file decodes.
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

        let pieces = split_for_spill(batch, self.spill_manager.max_batch_bytes)?;

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
        if let Some(InProgressWriter::Sync(writer)) = &mut self.writer {
            for (piece, _) in &pieces {
                // The writer calculates how many serialized bytes were emitted
                let (spilled_rows, delta_bytes) = writer.write(piece)?;

                self.spill_manager.metrics.spilled_rows.add(spilled_rows);
                self.spill_manager.metrics.spilled_bytes.add(delta_bytes);
            }
        } else if self.writer.is_some() {
            return Err(exec_datafusion_err!(
                "Cannot use synchronous append after asynchronous spill writing has started"
            ));
        }
        Ok(max_piece_size(&pieces))
    }

    /// Appends a `RecordBatch` using the asynchronous spill writer.
    pub async fn append_batch_async(&mut self, batch: &RecordBatch) -> Result<usize> {
        if self.in_progress_file.is_none() {
            return Err(exec_datafusion_err!(
                "Append operation failed: No active in-progress file. The file may have already been finalized."
            ));
        }

        let pieces = split_for_spill(batch, self.spill_manager.max_batch_bytes)?;

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

        match &mut self.writer {
            Some(InProgressWriter::Async(writer)) => {
                for (piece, _) in &pieces {
                    let (spilled_rows, delta_bytes) = writer.write(piece).await?;

                    self.spill_manager.metrics.spilled_rows.add(spilled_rows);
                    self.spill_manager.metrics.spilled_bytes.add(delta_bytes);
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
        Ok(max_piece_size(&pieces))
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

/// Compacts `batch` for spilling and, when `max_batch_bytes` is set, splits it into row
/// ranges of at most that size by recursive halving. Returns each piece with its post-GC
/// sliced size, which is what a reader measures when it decodes the piece.
///
/// The split is decided on [`referenced_size`], the bytes a row range points at, and only
/// the final pieces are compacted. The sliced size of an uncompacted slice counts every
/// buffer the slice keeps alive, so a slice of a large view array would report the whole
/// parent, and no split would ever seem to help.
///
/// When a batch is split, each piece also drops the dictionary values that none of its keys
/// use, so a large dictionary is divided between the pieces instead of written whole with
/// each one. The IPC stream then sends a replacement dictionary before each piece.
fn split_for_spill(
    batch: &RecordBatch,
    max_batch_bytes: Option<usize>,
) -> Result<Vec<(RecordBatch, usize)>> {
    let Some(max_batch_bytes) = max_batch_bytes else {
        return Ok(vec![compact_piece(batch, false)?]);
    };
    let size = referenced_batch_size(batch)?;
    if size <= max_batch_bytes || batch.num_rows() <= 1 {
        return Ok(vec![compact_piece(batch, false)?]);
    }
    let mut ranges = Vec::new();
    split_ranges(batch, size, max_batch_bytes, &mut ranges)?;
    if ranges.len() == 1 {
        return Ok(vec![compact_piece(batch, false)?]);
    }
    let mut pieces = Vec::with_capacity(ranges.len());
    for range in ranges {
        compact_range(&range, max_batch_bytes, &mut pieces)?;
    }
    Ok(pieces)
}

/// Compacts one range from [`split_ranges`]. The estimate can be lower than the compacted
/// size (a view builder allocates its data blocks with spare capacity), so a range that the
/// estimate put within the budget but that compacts to more is halved again. A range that
/// [`split_ranges`] kept over the budget on purpose (one row, or an undivided payload) is
/// written as it is.
fn compact_range(
    range: &RecordBatch,
    max_batch_bytes: usize,
    out: &mut Vec<(RecordBatch, usize)>,
) -> Result<()> {
    let (piece, size) = compact_piece(range, true)?;
    if size <= max_batch_bytes
        || range.num_rows() <= 1
        || referenced_batch_size(range)? > max_batch_bytes
    {
        out.push((piece, size));
        return Ok(());
    }
    let mid = range.num_rows() / 2;
    compact_range(&range.slice(0, mid), max_batch_bytes, out)?;
    compact_range(
        &range.slice(mid, range.num_rows() - mid),
        max_batch_bytes,
        out,
    )
}

/// Halves `batch` until each range is within `max_batch_bytes`, pushing the ranges in row
/// order.
///
/// Stops when a split does not divide the payload: neither half is within the budget and
/// the smaller half keeps more than three quarters of the parent's size. Splitting further
/// would only write more messages, without a smaller largest batch.
fn split_ranges(
    batch: &RecordBatch,
    size: usize,
    max_batch_bytes: usize,
    out: &mut Vec<RecordBatch>,
) -> Result<()> {
    if size <= max_batch_bytes || batch.num_rows() <= 1 {
        out.push(batch.clone());
        return Ok(());
    }
    let mid = batch.num_rows() / 2;
    let left = batch.slice(0, mid);
    let right = batch.slice(mid, batch.num_rows() - mid);
    let left_size = referenced_batch_size(&left)?;
    let right_size = referenced_batch_size(&right)?;
    let smaller_half = left_size.min(right_size);
    if smaller_half > max_batch_bytes && smaller_half > size - size / 4 {
        out.push(batch.clone());
        return Ok(());
    }
    split_ranges(&left, left_size, max_batch_bytes, out)?;
    split_ranges(&right, right_size, max_batch_bytes, out)
}

/// Compacts a piece for writing and returns it with its post-GC sliced size.
fn compact_piece(
    batch: &RecordBatch,
    gc_dictionaries: bool,
) -> Result<(RecordBatch, usize)> {
    let batch = if gc_dictionaries {
        let columns = batch
            .columns()
            .iter()
            .map(|array| match array.data_type() {
                DataType::Dictionary(_, _) => {
                    Ok(garbage_collect_any_dictionary(array.as_any_dictionary())?)
                }
                _ => Ok(Arc::clone(array)),
            })
            .collect::<Result<Vec<ArrayRef>>>()?;
        RecordBatch::try_new(batch.schema(), columns)?
    } else {
        batch.clone()
    };
    let gc_batch = gc_view_arrays(&batch)?;
    let size = gc_batch.get_sliced_size()?;
    Ok((gc_batch, size))
}

fn referenced_batch_size(batch: &RecordBatch) -> Result<usize> {
    batch
        .columns()
        .iter()
        .map(referenced_size)
        .sum::<Result<usize>>()
}

/// Estimates the bytes that a (possibly sliced) array points at, which is about its size
/// once compacted.
///
/// - View arrays: 16 bytes per view, plus each value too long to be stored inline. The
///   shared data buffers are not counted, because compaction copies out only these values.
/// - Dictionaries: the keys, plus the values that the keys use. Compaction of a split piece
///   drops the rest.
/// - Other types: [`arrow::array::ArrayData::get_slice_memory_size`].
fn referenced_size(array: &ArrayRef) -> Result<usize> {
    let nulls = array
        .nulls()
        .map_or(0, |n| n.buffer().len().min(n.len().div_ceil(8)));
    let size = match array.data_type() {
        DataType::Utf8View => nulls + view_values_size(array.as_string_view()),
        DataType::BinaryView => nulls + view_values_size(array.as_binary_view()),
        DataType::Dictionary(_, _) => {
            let dictionary = array.as_any_dictionary();
            let keys = dictionary.keys().to_data().get_slice_memory_size()?;
            let used = garbage_collect_any_dictionary(dictionary)?;
            keys + referenced_size(used.as_any_dictionary().values())?
        }
        _ => array.to_data().get_slice_memory_size()?,
    };
    Ok(size)
}

fn view_values_size<T: ByteViewType + ?Sized>(array: &GenericByteViewArray<T>) -> usize {
    let long_values: usize = array
        .views()
        .iter()
        .map(|view| {
            let len = *view as u32 as usize;
            if len > MAX_INLINE_VIEW_LEN as usize {
                len
            } else {
                0
            }
        })
        .sum();
    array.len() * size_of::<u128>() + long_values
}

fn max_piece_size(pieces: &[(RecordBatch, usize)]) -> usize {
    pieces.iter().map(|(_, size)| *size).max().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        DictionaryArray, Int32Array, Int64Array, StringArray, StringViewArray,
    };
    use arrow_schema::{Field, Schema, SchemaRef};
    use datafusion_common::DataFusionError;
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

    fn manager(
        schema: SchemaRef,
        max_batch_bytes: Option<usize>,
    ) -> Result<Arc<SpillManager>> {
        let runtime = Arc::new(RuntimeEnvBuilder::new().build()?);
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        Ok(Arc::new(
            SpillManager::new(runtime, metrics, schema)
                .with_max_batch_bytes(max_batch_bytes),
        ))
    }

    /// Writes `batches` through a bounded spill file, reads the file back with the reader's
    /// size check on, and returns what was read and the recorded largest batch.
    async fn round_trip(
        batches: &[RecordBatch],
        max_batch_bytes: usize,
    ) -> Result<(Vec<RecordBatch>, usize)> {
        let manager = manager(batches[0].schema(), Some(max_batch_bytes))?;
        let (file, max_memory) = manager
            .spill_record_batch_iter_and_return_max_batch_memory(
                batches.iter().map(Ok::<_, DataFusionError>),
                "test",
            )?
            .unwrap();
        let read = manager
            .read_spill_as_stream(file, Some(max_memory))?
            .try_collect::<Vec<_>>()
            .await?;
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

    /// Repeated values are deduplicated within each piece, so the pieces do not add up to the
    /// parent. The bound and the row order must still hold.
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
        let manager = manager(batch.schema(), None)?;
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
}
