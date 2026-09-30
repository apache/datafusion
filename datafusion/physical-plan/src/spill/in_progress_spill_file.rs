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

use arrow::array::{
    AnyDictionaryArray, Array, ArrayRef, AsArray, BooleanBufferBuilder,
    FixedSizeListArray, GenericByteArray, GenericByteViewArray, GenericListArray,
    GenericListViewArray, MapArray, OffsetSizeTrait, RecordBatch, RecordBatchOptions,
    StructArray, make_array,
};
use arrow::buffer::{BooleanBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{
    ArrowNativeType, ByteArrayType, ByteViewType, DataType, FieldRef,
};
use arrow_data::{ArrayData, MAX_INLINE_VIEW_LEN, transform::MutableArrayData};
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
        let max_rows = pieces.iter().map(|(piece, _)| piece.num_rows()).max();
        Ok((max_piece_size(&pieces), max_rows.unwrap_or(0)))
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
/// ranges of at most that size by recursive halving. Returns each piece with the size that
/// a reader measures when it decodes the piece.
///
/// Without a bound, the batch is written whole after [`gc_view_arrays`], as before.
///
/// With a bound, the split is decided on [`referenced_size`], which models what the IPC
/// writer encodes for a row range, and each range is then compacted by [`compact_array`]
/// so that it matches that model. The sliced size of an uncompacted slice counts every
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
        let gc_batch = gc_view_arrays(batch)?;
        let size = gc_batch.get_sliced_size()?;
        return Ok(vec![(gc_batch, size)]);
    };
    let size = referenced_batch_size(batch)?;
    let mut ranges = Vec::new();
    split_ranges(batch, size, max_batch_bytes, &mut ranges)?;
    ranges.iter().map(compact_piece).collect()
}

/// The size recorded for `batch` when a bounded spill file writes it unsplit.
pub(crate) fn bounded_spill_size(batch: &RecordBatch) -> Result<usize> {
    Ok(compact_piece(batch)?.1)
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

/// Compacts a piece for writing with [`compact_array`] and returns it with the size of
/// the batch a reader decodes: its [`referenced_batch_size`] plus the IPC padding.
fn compact_piece(batch: &RecordBatch) -> Result<(RecordBatch, usize)> {
    let columns = batch
        .columns()
        .iter()
        .map(compact_array)
        .collect::<Result<Vec<_>>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    let piece = RecordBatch::try_new_with_options(batch.schema(), columns, &options)?;
    let padding: usize = piece
        .columns()
        .iter()
        .map(|column| ipc_padding(&column.to_data()))
        .sum();
    let size = referenced_batch_size(&piece)? + padding;
    Ok((piece, size))
}

/// Rewrites the parts of a (possibly sliced) array that the IPC writer would encode whole,
/// so that what is written is what [`referenced_size`] counts:
///
/// - View arrays keep every data buffer they point into: they are garbage collected.
/// - Dictionaries keep all their values: the unused values are dropped.
/// - List views keep their whole child: the used child ranges are copied out.
///
/// The IPC writer already narrows flat arrays and the children of lists and maps to the
/// slice, so those are left as they are unless a descendant needs one of the rewrites
/// above. Other types (unions, run-end encoded arrays) are written as they are.
fn compact_array(array: &ArrayRef) -> Result<ArrayRef> {
    if !needs_compaction(array.data_type()) {
        return Ok(Arc::clone(array));
    }
    let compacted: ArrayRef = match array.data_type() {
        DataType::Utf8View => compact_view(array.as_string_view()),
        DataType::BinaryView => compact_view(array.as_binary_view()),
        DataType::Dictionary(_, _) => {
            // Returns the array as it is when every value is used.
            let gc = garbage_collect_any_dictionary(array.as_any_dictionary())?;
            let dictionary = gc.as_any_dictionary();
            dictionary.with_values(compact_array(dictionary.values())?)
        }
        DataType::List(field) => compact_list(Arc::clone(field), array.as_list::<i32>())?,
        DataType::LargeList(field) => {
            compact_list(Arc::clone(field), array.as_list::<i64>())?
        }
        DataType::Map(field, ordered) => {
            let map = array.as_map();
            let (start, len) = used_child_range(map.value_offsets());
            let entries: ArrayRef = Arc::new(map.entries().slice(start, len));
            let entries = compact_array(&entries)?.as_struct().clone();
            Arc::new(MapArray::try_new(
                Arc::clone(field),
                rebase_offsets(map.offsets()),
                entries,
                map.nulls().cloned(),
                *ordered,
            )?)
        }
        DataType::Struct(fields) => {
            let structs = array.as_struct();
            let columns = structs
                .columns()
                .iter()
                .map(compact_array)
                .collect::<Result<Vec<_>>>()?;
            Arc::new(StructArray::try_new_with_length(
                fields.clone(),
                columns,
                structs.nulls().cloned(),
                structs.len(),
            )?)
        }
        DataType::FixedSizeList(field, size) => {
            let list = array.as_fixed_size_list();
            Arc::new(FixedSizeListArray::try_new(
                Arc::clone(field),
                *size,
                compact_array(list.values())?,
                list.nulls().cloned(),
            )?)
        }
        DataType::ListView(field) => {
            compact_list_view(Arc::clone(field), array.as_list_view::<i32>())?
        }
        DataType::LargeListView(field) => {
            compact_list_view(Arc::clone(field), array.as_list_view::<i64>())?
        }
        _ => Arc::clone(array),
    };
    Ok(compacted)
}

/// Whether [`compact_array`] rewrites arrays of this type, which it does when the type or
/// one of its descendants is a view array, a dictionary or a list view.
fn needs_compaction(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8View
        | DataType::BinaryView
        | DataType::Dictionary(_, _)
        | DataType::ListView(_)
        | DataType::LargeListView(_) => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => needs_compaction(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| needs_compaction(field.data_type())),
        _ => false,
    }
}

/// Garbage collects a view array whose data buffers hold more than its long values.
fn compact_view<T: ByteViewType + ?Sized>(array: &GenericByteViewArray<T>) -> ArrayRef {
    let buffers: usize = array.data_buffers().iter().map(|b| b.len()).sum();
    if buffers > array.total_buffer_bytes_used() {
        Arc::new(array.gc())
    } else {
        Arc::new(array.clone())
    }
}

/// Narrows a list to the child range its offsets use, and compacts that range.
fn compact_list<O: OffsetSizeTrait>(
    field: FieldRef,
    list: &GenericListArray<O>,
) -> Result<ArrayRef> {
    let (start, len) = used_child_range(list.value_offsets());
    let values = compact_array(&list.values().slice(start, len))?;
    Ok(Arc::new(GenericListArray::<O>::try_new(
        field,
        rebase_offsets(list.offsets()),
        values,
        list.nulls().cloned(),
    )?))
}

/// Copies the child range of each list view row into a new child, in row order.
fn compact_list_view<O: OffsetSizeTrait>(
    field: FieldRef,
    list: &GenericListViewArray<O>,
) -> Result<ArrayRef> {
    let child = list.values().to_data();
    let total: usize = list.value_sizes().iter().map(|size| size.as_usize()).sum();
    let mut values = MutableArrayData::new(vec![&child], false, total);
    let mut offsets = Vec::with_capacity(list.len());
    let mut position = 0;
    for (offset, size) in list.value_offsets().iter().zip(list.value_sizes()) {
        offsets.push(O::from_usize(position).ok_or_else(|| {
            exec_datafusion_err!("list view offset {position} overflows while spilling")
        })?);
        let (offset, size) = (offset.as_usize(), size.as_usize());
        if size > 0 {
            values.try_extend(0, offset, offset + size)?;
        }
        position += size;
    }
    let values = compact_array(&make_array(values.freeze()))?;
    Ok(Arc::new(GenericListViewArray::<O>::try_new(
        field,
        ScalarBuffer::from(offsets),
        list.sizes().clone(),
        values,
        list.nulls().cloned(),
    )?))
}

/// The start and length of the child range that list offsets use.
fn used_child_range<O: OffsetSizeTrait>(offsets: &[O]) -> (usize, usize) {
    match (offsets.first(), offsets.last()) {
        (Some(first), Some(last)) => {
            (first.as_usize(), last.as_usize() - first.as_usize())
        }
        _ => (0, 0),
    }
}

/// List offsets that start at zero, for a child narrowed with [`used_child_range`].
fn rebase_offsets<O: OffsetSizeTrait>(offsets: &OffsetBuffer<O>) -> OffsetBuffer<O> {
    let first = offsets[0].as_usize();
    if first == 0 {
        return offsets.clone();
    }
    let rebased = offsets.iter().map(|o| O::usize_as(o.as_usize() - first));
    OffsetBuffer::new(ScalarBuffer::from_iter(rebased))
}

fn referenced_batch_size(batch: &RecordBatch) -> Result<usize> {
    batch
        .columns()
        .iter()
        .map(referenced_size)
        .sum::<Result<usize>>()
}

/// Estimates the bytes of a (possibly sliced) array once [`compact_array`] compacts it and
/// the IPC writer writes it: about the size a reader decodes.
///
/// [`arrow::array::ArrayData::get_slice_memory_size`] narrows only the top level of a
/// slice: a sliced list or map reports its whole child, and a sliced view array reports all
/// the data buffers it keeps alive. Both halves of a split would then report about the size
/// of the parent, and the split would stop. So:
///
/// - View arrays: 16 bytes per view, plus each value too long to be stored inline.
/// - Dictionaries: the keys, plus the values that the keys use.
/// - Lists, large lists and maps: the offsets, plus the child range that the offsets use.
/// - List views: the offsets and sizes, plus the child range of each row.
/// - Structs and fixed-size lists: their children, which `slice` already narrows.
/// - Other types: `get_slice_memory_size`, which is exact for flat types. Unions and
///   run-end encoded arrays are counted with all their children.
fn referenced_size(array: &ArrayRef) -> Result<usize> {
    let nulls = array
        .nulls()
        .map_or(0, |n| n.buffer().len().min(n.len().div_ceil(8)));
    let size = match array.data_type() {
        DataType::Utf8View => nulls + view_array_size(array.as_string_view()),
        DataType::BinaryView => nulls + view_array_size(array.as_binary_view()),
        DataType::Dictionary(_, _) => {
            let dictionary = array.as_any_dictionary();
            let keys = dictionary.keys().to_data().get_slice_memory_size()?;
            keys + dictionary_values_size(dictionary)?
        }
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            nulls + list_size(list.value_offsets(), list.values())?
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            nulls + list_size(list.value_offsets(), list.values())?
        }
        DataType::Map(_, _) => {
            let map = array.as_map();
            let entries: ArrayRef = Arc::new(map.entries().clone());
            nulls + list_size(map.value_offsets(), &entries)?
        }
        DataType::ListView(_) => nulls + list_view_size(array.as_list_view::<i32>())?,
        DataType::LargeListView(_) => {
            nulls + list_view_size(array.as_list_view::<i64>())?
        }
        DataType::Struct(_) => {
            nulls
                + array
                    .as_struct()
                    .columns()
                    .iter()
                    .map(referenced_size)
                    .sum::<Result<usize>>()?
        }
        DataType::FixedSizeList(_, _) => {
            nulls + referenced_size(array.as_fixed_size_list().values())?
        }
        _ => array.to_data().get_slice_memory_size()?,
    };
    Ok(size)
}

/// The offsets of a list or map, plus the size of the child range that they use.
fn list_size<O: OffsetSizeTrait>(offsets: &[O], child: &ArrayRef) -> Result<usize> {
    let (start, len) = used_child_range(offsets);
    Ok(size_of_val(offsets) + referenced_size(&child.slice(start, len))?)
}

/// The offsets and sizes of a list view, plus the child range of each row.
fn list_view_size<O: OffsetSizeTrait>(list: &GenericListViewArray<O>) -> Result<usize> {
    let mut size = size_of_val(list.value_offsets()) + size_of_val(list.value_sizes());
    for (offset, len) in list.value_offsets().iter().zip(list.value_sizes()) {
        if len.as_usize() > 0 {
            size +=
                referenced_size(&list.values().slice(offset.as_usize(), len.as_usize()))?;
        }
    }
    Ok(size)
}

fn view_array_size<T: ByteViewType + ?Sized>(array: &GenericByteViewArray<T>) -> usize {
    array.len() * size_of::<u128>() + array.total_buffer_bytes_used()
}

/// An upper bound on the padding that the IPC writer adds after each buffer of `data`,
/// which a reader keeps: buffers are written aligned to 64 bytes.
fn ipc_padding(data: &ArrayData) -> usize {
    const IPC_ALIGNMENT: usize = 64;
    let buffers = data.buffers().len() + usize::from(data.nulls().is_some());
    buffers * (IPC_ALIGNMENT - 1)
        + data.child_data().iter().map(ipc_padding).sum::<usize>()
}

/// Marks the dictionary values that at least one non-null key uses.
fn used_dictionary_values(dictionary: &dyn AnyDictionaryArray) -> BooleanBuffer {
    let values_len = dictionary.values().len();
    let mut used = BooleanBufferBuilder::new(values_len);
    used.append_n(values_len, false);
    if values_len == 0 {
        // `normalized_keys` asserts that there are values.
        return used.finish();
    }
    let keys = dictionary.keys();
    for (row, key) in dictionary.normalized_keys().into_iter().enumerate() {
        if key < values_len && keys.is_valid(row) {
            used.set_bit(key, true);
        }
    }
    used.finish()
}

/// The size of the dictionary values that the keys use, which is what is left of the
/// values after [`compact_array`] drops the unused ones.
///
/// Measured from a bitmap of the used values for byte, view and primitive values, so it
/// allocates one bit per value instead of copying the used values out. Other value types
/// fall back to garbage collecting the dictionary.
fn dictionary_values_size(dictionary: &dyn AnyDictionaryArray) -> Result<usize> {
    let values = dictionary.values();
    let used = used_dictionary_values(dictionary);
    let used_count = used.count_set_bits();
    if used_count == values.len() {
        return referenced_size(values);
    }
    let size = match values.data_type() {
        DataType::Utf8 => byte_values_size(values.as_string::<i32>(), &used),
        DataType::LargeUtf8 => byte_values_size(values.as_string::<i64>(), &used),
        DataType::Binary => byte_values_size(values.as_binary::<i32>(), &used),
        DataType::LargeBinary => byte_values_size(values.as_binary::<i64>(), &used),
        DataType::Utf8View => view_values_size(values.as_string_view(), &used),
        DataType::BinaryView => view_values_size(values.as_binary_view(), &used),
        data_type if data_type.is_primitive() => {
            used_count * data_type.primitive_width().unwrap_or(0)
        }
        _ => {
            let gc = garbage_collect_any_dictionary(dictionary)?;
            return referenced_size(gc.as_any_dictionary().values());
        }
    };
    let nulls = values.nulls().map_or(0, |_| used_count.div_ceil(8));
    Ok(nulls + size)
}

fn byte_values_size<T: ByteArrayType>(
    values: &GenericByteArray<T>,
    used: &BooleanBuffer,
) -> usize {
    let offset_size = size_of::<T::Offset>();
    let lengths: usize = used
        .set_indices()
        .map(|i| values.value_length(i).as_usize() + offset_size)
        .sum();
    lengths + offset_size
}

fn view_values_size<T: ByteViewType + ?Sized>(
    values: &GenericByteViewArray<T>,
    used: &BooleanBuffer,
) -> usize {
    let views = values.views();
    used.set_indices()
        .map(|i| {
            let len = views[i] as u32 as usize;
            let long = if len > MAX_INLINE_VIEW_LEN as usize {
                len
            } else {
                0
            };
            size_of::<u128>() + long
        })
        .sum()
}

fn max_piece_size(pieces: &[(RecordBatch, usize)]) -> usize {
    pieces.iter().map(|(_, size)| *size).max().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        DictionaryArray, Int32Array, Int64Array, ListBuilder, MapBuilder, StringArray,
        StringBuilder, StringViewArray, StructArray,
    };
    use arrow_schema::{Field, Fields, Schema, SchemaRef};
    use datafusion_common::DataFusionError;
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
        // The reader only logs a batch larger than the recorded size, so check it here:
        // the merge reserves memory from the recorded size. A decoded batch shares one
        // allocation per IPC message, which `get_sliced_size` counts once per view data
        // buffer, so compare the memory the batch actually holds.
        for batch in &read {
            let size = get_record_batch_memory_size(batch);
            assert!(
                size <= max_memory,
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
    /// when a sort emits a chunk. It is within the budget and not split, but it must still
    /// drop the unused values, or the reader decodes the whole dictionary with it.
    #[tokio::test]
    async fn unsplit_batch_drops_unused_dictionary_values() -> Result<()> {
        let values = StringArray::from_iter_values((0..256).map(wide_value));
        let keys = Int32Array::from_iter_values([3, 7, 7, 200]);
        let dictionary = DictionaryArray::try_new(keys, Arc::new(values))?;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "d",
                dictionary_type(DataType::Utf8),
                false,
            )])),
            vec![Arc::new(dictionary)],
        )?;
        let budget = 1024 * 1024;
        let (read, max_memory) = round_trip(std::slice::from_ref(&batch), budget).await?;
        assert_eq!(read.len(), 1);
        assert!(
            max_memory <= 4 * WIDE,
            "recorded {max_memory} bytes for 3 used values"
        );
        assert_eq!(read[0].column(0).as_any_dictionary().values().len(), 3);
        Ok(())
    }

    /// A dictionary inside a struct is compacted like a top-level one, so halving shrinks
    /// the pieces instead of writing one-row pieces that each carry the whole dictionary.
    #[tokio::test]
    async fn nested_dictionary_is_divided() -> Result<()> {
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
        let (read, max_memory) = round_trip(std::slice::from_ref(&batch), budget).await?;
        assert!(
            max_memory <= budget,
            "largest spilled batch is {max_memory} bytes"
        );
        assert!(
            (8..=32).contains(&read.len()),
            "expected 8 to 32 pieces, got {}",
            read.len()
        );
        Ok(())
    }

    /// A list view keeps its whole child when it is sliced, and the IPC writer writes that
    /// child whole, so each piece must copy out only the child ranges its rows use.
    #[tokio::test]
    async fn list_view_column_is_split() -> Result<()> {
        let list = wide_list(256);
        let list_view: ArrayRef = Arc::new(arrow::array::ListViewArray::from(
            list.as_list::<i32>().clone(),
        ));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "l",
            list_view.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![list_view.slice(64, 128)])?;
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
        let manager = manager(batch.schema(), Some(4 * WIDE + WIDE / 2))?;
        let mut file = manager.create_in_progress_file("test")?;
        let (size, rows) = file.append_batch_async_with_stats(&batch).await?;
        assert!(size <= 4 * WIDE + WIDE / 2);
        assert_eq!(rows, 4);
        Ok(())
    }

    /// A piece whose dictionary keys are all null has no used values. After the unused
    /// values are dropped the dictionary is empty, which must not panic when measured.
    #[tokio::test]
    async fn dictionary_with_only_null_keys() -> Result<()> {
        let values = StringArray::from_iter_values((0..64).map(wide_value));
        let keys = Int32Array::from_iter((0..64).map(|i| (i >= 8).then_some(i)));
        let dictionary: ArrayRef =
            Arc::new(DictionaryArray::try_new(keys, Arc::new(values))?);
        let fields =
            Fields::from(vec![Field::new("d", dictionary_type(DataType::Utf8), true)]);
        let structs: ArrayRef = Arc::new(StructArray::try_new(
            fields.clone(),
            vec![Arc::clone(&dictionary)],
            None,
        )?);
        let schema = Arc::new(Schema::new(vec![
            Field::new("d", dictionary_type(DataType::Utf8), true),
            Field::new("s", DataType::Struct(fields), false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![dictionary, structs])?;
        // Pieces of 8 rows: the first has only null keys.
        let budget = 20 * WIDE;
        for batch in [batch.slice(0, 8), batch.slice(0, 0), batch] {
            if batch.num_rows() == 0 {
                split_for_spill(&batch, Some(budget))?;
                continue;
            }
            let (read, _) = round_trip(std::slice::from_ref(&batch), budget).await?;
            assert_same_rows(&read, &[batch]);
        }
        Ok(())
    }
}
