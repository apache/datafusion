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

use crate::sorts::{
    ProgressStats, interleave_memory_size_from_sliced, schema_needs_recursive_read_bound,
};
use datafusion_common::{Result, internal_datafusion_err};
use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion_common::exec_datafusion_err;
use datafusion_execution::spill_file::SpillFile;

use super::{
    AsyncIPCStreamWriter, IPCStreamEncoder, IPCStreamWriter, gc_view_arrays,
    spill_manager::{GetSlicedSize, SpillManager},
};

/// Separate the serialized size, physical read bound, and new construction
/// buffers. Flat schemas keep their existing logical read estimate.
pub(crate) struct SpillBatchMemory {
    pub sliced: usize,
    pub decoded: Option<usize>,
    pub construction: usize,
    pub progress: Option<ProgressStats>,
}

impl SpillBatchMemory {
    pub fn input_memory_size(&self) -> usize {
        // Recursive input grants cover physical reads. Construction has its own
        // reservation; multiplying it into every read slot over-reserves memory.
        self.decoded
            .unwrap_or_else(|| self.sliced.max(self.construction))
    }
}

#[derive(Default)]
pub(crate) struct SpillFileMemory {
    pub input: usize,
    pub progress: Option<ProgressStats>,
    batches: usize,
}

impl SpillFileMemory {
    pub fn include(&mut self, batch: &SpillBatchMemory) {
        self.input = self.input.max(batch.input_memory_size());
        if self.batches == 0 {
            self.progress = batch.progress;
        } else {
            match (&mut self.progress, batch.progress) {
                (Some(total), Some(current)) => total.include(current),
                _ => self.progress = None,
            }
        }
        self.batches += 1;
    }
}

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
    /// Returns the post-GC sliced memory size of the batch for memory accounting.
    ///
    /// # Errors
    /// - Returns an error if the file is not active (has been finalized)
    /// - Returns an error if appending would exceed the disk usage limit configured
    ///   by `max_temp_directory_size` in `DiskManager`
    pub fn append_batch(&mut self, batch: &RecordBatch) -> Result<usize> {
        self.append_batch_inner(batch, false)
            .map(|memory| memory.sliced)
    }

    /// Report serialized size, physical read bound and new construction buffers
    /// from the post-GC arrays and messages that are actually written.
    pub(crate) fn append_batch_with_memory_size(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<SpillBatchMemory> {
        self.append_batch_inner(batch, true)
    }

    fn append_batch_inner(
        &mut self,
        batch: &RecordBatch,
        collect_stats: bool,
    ) -> Result<SpillBatchMemory> {
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
            // Compact-only public writes do not need the extra statistics.
            writer.encoder.track_read_memory = collect_stats;
            // The writer calculates how many serialized bytes were emitted
            let (spilled_rows, delta_bytes) = writer.write(&gc_batch)?;

            self.spill_manager.metrics.spilled_rows.add(spilled_rows);
            self.spill_manager.metrics.spilled_bytes.add(delta_bytes);
        } else if self.writer.is_some() {
            return Err(exec_datafusion_err!(
                "Cannot use synchronous append after asynchronous spill writing has started"
            ));
        }
        let sliced_size = gc_batch.get_sliced_size()?;
        let decoded = match &self.writer {
            Some(InProgressWriter::Sync(writer)) => writer.encoder.read_memory_bound,
            Some(InProgressWriter::Async(writer)) => writer.encoder.read_memory_bound,
            None => None,
        };
        let construction = if collect_stats {
            interleave_memory_size_from_sliced(
                &gc_batch,
                if decoded.is_some() { 0 } else { sliced_size },
            )
            .unwrap_or(sliced_size)
        } else {
            sliced_size
        };
        let progress = if collect_stats
            && schema_needs_recursive_read_bound(gc_batch.schema().as_ref())
        {
            Some(ProgressStats::from_batch(&gc_batch).ok_or_else(|| {
                exec_datafusion_err!("Spill merge progress estimate exceeds usize::MAX")
            })?)
        } else {
            None
        };
        Ok(SpillBatchMemory {
            sliced: sliced_size,
            decoded,
            construction,
            progress,
        })
    }

    /// Appends a `RecordBatch` using the asynchronous spill writer.
    pub async fn append_batch_async(&mut self, batch: &RecordBatch) -> Result<usize> {
        self.append_batch_async_inner(batch, false)
            .await
            .map(|memory| memory.sliced)
    }

    /// Asynchronous counterpart using the arrays passed to the writer after GC.
    pub(crate) async fn append_batch_async_with_memory_size(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<SpillBatchMemory> {
        self.append_batch_async_inner(batch, true).await
    }

    async fn append_batch_async_inner(
        &mut self,
        batch: &RecordBatch,
        collect_stats: bool,
    ) -> Result<SpillBatchMemory> {
        if self.in_progress_file.is_none() {
            return Err(exec_datafusion_err!(
                "Append operation failed: No active in-progress file. The file may have already been finalized."
            ));
        }

        let gc_batch = gc_view_arrays(batch)?;

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
                writer.encoder.track_read_memory = collect_stats;
                let (spilled_rows, delta_bytes) = writer.write(&gc_batch).await?;

                self.spill_manager.metrics.spilled_rows.add(spilled_rows);
                self.spill_manager.metrics.spilled_bytes.add(delta_bytes);
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
        let sliced_size = gc_batch.get_sliced_size()?;
        let decoded = match &self.writer {
            Some(InProgressWriter::Sync(writer)) => writer.encoder.read_memory_bound,
            Some(InProgressWriter::Async(writer)) => writer.encoder.read_memory_bound,
            None => None,
        };
        let construction = if collect_stats {
            interleave_memory_size_from_sliced(
                &gc_batch,
                if decoded.is_some() { 0 } else { sliced_size },
            )
            .unwrap_or(sliced_size)
        } else {
            sliced_size
        };
        let progress = if collect_stats
            && schema_needs_recursive_read_bound(gc_batch.schema().as_ref())
        {
            Some(ProgressStats::from_batch(&gc_batch).ok_or_else(|| {
                exec_datafusion_err!("Spill merge progress estimate exceeds usize::MAX")
            })?)
        } else {
            None
        };
        Ok(SpillBatchMemory {
            sliced: sliced_size,
            decoded,
            construction,
            progress,
        })
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
    use arrow::array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_physical_expr_common::metrics::{
        ExecutionPlanMetricsSet, SpillMetrics,
    };
    use futures::TryStreamExt;

    #[tokio::test]
    async fn recursive_read_bound_covers_a_body_above_64_mib() -> Result<()> {
        use crate::spill::get_record_batch_memory_size;
        use arrow::array::{ArrayRef, BinaryViewArray};
        use arrow::buffer::Buffer;
        let bytes = 65 * 1024 * 1024 + 1;
        let view = arrow_data::ByteView::new(bytes as u32, b"xxxx").as_u128();
        let values: ArrayRef = Arc::new(BinaryViewArray::try_new(
            vec![view].into(),
            vec![Buffer::from(vec![b'x'; bytes])],
            None,
        )?);
        let input = RecordBatch::try_from_iter(vec![("binary", values)])?;
        let env = RuntimeEnvBuilder::new().build_arc()?;
        let manager = Arc::new(SpillManager::new(
            env,
            SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
            input.schema(),
        ));
        let mut writer = manager.create_in_progress_file("large IPC body")?;
        let stats = writer.append_batch_with_memory_size(&input)?;
        let file = writer.finish()?.unwrap();
        let output: Vec<RecordBatch> = manager
            .read_spill_as_stream(file, None)?
            .try_collect()
            .await?;
        assert_eq!(output, vec![input]);
        let retained = get_record_batch_memory_size(&output[0]);
        assert!(retained > 64 * 1024 * 1024);
        // This reader uses its own exact-capacity framer, not read_body_bounded.
        assert!(retained < 66 * 1024 * 1024);
        assert!(stats.decoded.unwrap() >= retained);
        Ok(())
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn written_batch_memory_uses_post_gc_arrays_and_physical_ipc_bounds(
        #[values(false, true)] asynchronous: bool,
        #[values("view", "nested", "wide", "entropy", "utf8", "empty_nested")] kind: &str,
        #[values(
            datafusion_common::config::SpillCompression::Uncompressed,
            datafusion_common::config::SpillCompression::Lz4Frame,
            datafusion_common::config::SpillCompression::Zstd
        )]
        compression: datafusion_common::config::SpillCompression,
    ) -> Result<()> {
        use crate::spill::get_record_batch_memory_size;
        use arrow::array::{
            Array, ArrayRef, BinaryViewArray, BooleanArray, ListArray, StringArray,
            StringViewArray, StructArray,
        };
        use arrow::buffer::OffsetBuffer;
        let rows = 2048;
        let strings = (0..rows)
            .map(|row| format!("{row:08}{}", "x".repeat(55)))
            .collect::<Vec<_>>();
        let values: ArrayRef = if kind == "utf8" {
            Arc::new(StringArray::from(strings))
        } else if kind == "entropy" {
            let mut seed = 0x1234_5678_9abc_def0_u64;
            Arc::new(BinaryViewArray::from_iter_values((0..rows).map(|_| {
                (0..63)
                    .map(|_| {
                        seed ^= seed << 13;
                        seed ^= seed >> 7;
                        seed ^= seed << 17;
                        seed as u8
                    })
                    .collect::<Vec<_>>()
            })))
        } else {
            Arc::new(StringViewArray::from(strings))
        };
        let values: ArrayRef = if kind == "empty_nested" {
            Arc::new(ListArray::new(
                Arc::new(Field::new_list_field(DataType::Utf8, false)),
                OffsetBuffer::new(vec![0i32; rows as usize + 1].into()),
                Arc::new(StringArray::from(Vec::<&str>::new())),
                None,
            ))
        } else if kind == "nested" {
            Arc::new(ListArray::new(
                Arc::new(Field::new_list_field(DataType::Utf8View, false)),
                OffsetBuffer::new((0..=rows).collect::<Vec<_>>().into()),
                values,
                None,
            ))
        } else if kind == "wide" {
            let mut columns = (0..64)
                .map(|n| {
                    Arc::new(BooleanArray::from_iter(
                        (0..rows).map(|row| (row % 3 != 0).then_some((row + n) % 2 == 0)),
                    )) as ArrayRef
                })
                .collect::<Vec<_>>();
            columns.push(values);
            let fields = columns
                .iter()
                .enumerate()
                .map(|(n, a)| {
                    Arc::new(Field::new(format!("f{n}"), a.data_type().clone(), true))
                })
                .collect::<Vec<_>>()
                .into();
            Arc::new(StructArray::new(fields, columns, None))
        } else {
            values
        };
        let original = RecordBatch::try_from_iter(vec![("value", values)])?;
        let env = RuntimeEnvBuilder::new().build_arc()?;
        let manager = Arc::new(
            SpillManager::new(
                env,
                SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
                original.schema(),
            )
            .with_compression_type(compression),
        );
        // Re-read an IPC body so the source's distinct Buffer slices alias one
        // allocation. Taking a short prefix must not record that old capacity.
        let file = manager
            .spill_record_batch_and_finish(std::slice::from_ref(&original), "input")?
            .unwrap();
        let decoded: Vec<RecordBatch> = manager
            .read_spill_as_stream(file, None)?
            .try_collect()
            .await?;
        let input = decoded[0].slice(0, 64);
        let gc = gc_view_arrays(&input)?;
        let sliced = gc.get_sliced_size()?;
        let mut writer = manager.create_in_progress_file("output")?;
        let stats = if asynchronous {
            writer.append_batch_async_with_memory_size(&input).await?
        } else {
            writer.append_batch_with_memory_size(&input)?
        };
        assert_eq!(stats.sliced, sliced);
        let file = if asynchronous {
            writer.finish_async().await?
        } else {
            writer.finish()?
        }
        .unwrap();
        let read: Vec<RecordBatch> = manager
            .read_spill_as_stream(file, None)?
            .try_collect()
            .await?;
        assert_eq!(read, vec![input.clone()]);
        if kind == "utf8" {
            assert_eq!(stats.progress, None);
            assert_eq!(stats.decoded, None);
            assert_eq!(
                stats.input_memory_size(),
                interleave_memory_size_from_sliced(&input, sliced).unwrap()
            );
        } else {
            if kind != "nested" {
                assert!(sliced < get_record_batch_memory_size(&input));
            }
            assert_eq!(
                stats.construction,
                interleave_memory_size_from_sliced(&gc, 0).unwrap()
            );
            // No tolerance: this bound includes padded IPC bodies and possible
            // independent decompression/alignment buffers, counted separately.
            assert!(stats.decoded.unwrap() >= get_record_batch_memory_size(&read[0]));
            let measured = ProgressStats::from_batch(&read[0]).unwrap();
            let written = stats.progress.unwrap();
            assert!(
                measured.workspace <= written.workspace,
                "decoded W: {measured:?} > written {written:?}"
            );
            assert!(
                measured.row_content <= written.row_content,
                "decoded P: {measured:?} > written {written:?}"
            );
            if kind != "nested" {
                assert!(stats.input_memory_size() < get_record_batch_memory_size(&input));
            }
        }
        Ok(())
    }

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
