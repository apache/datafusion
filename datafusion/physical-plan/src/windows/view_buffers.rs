// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Pruning of the batches [`BoundedWindowAggExec`] keeps across input batches.
//!
//! The operator extends them with `concat` and drops the rows it is done with.
//! A slice of a `Utf8View` / `BinaryView` array keeps the whole data buffer
//! list and `concat` copies the lists of its inputs, so with plain slices the
//! list would grow by the buffers of every batch received. [`prune_batch_prefix`]
//! rewrites the kept view columns to list only the buffers they reference.
//!
//! [`BoundedWindowAggExec`]: super::BoundedWindowAggExec

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, GenericByteViewArray, RecordBatchOptions, new_empty_array,
};
use arrow::buffer::{Buffer, ScalarBuffer};
use arrow::datatypes::{ByteViewType, DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_data::{ByteView, MAX_INLINE_VIEW_LEN};
use datafusion_common::{HashMap, Result};

/// See [`should_compact`].
const ROWS_PER_NEW_ENTRY: usize = 4096;

/// The indices of the `Utf8View` and `BinaryView` columns of `schema`.
pub(super) fn view_column_indices(schema: &SchemaRef) -> Vec<usize> {
    schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| {
            matches!(field.data_type(), DataType::Utf8View | DataType::BinaryView)
        })
        .map(|(index, _)| index)
        .collect()
}

/// Whether a view column with `n_rows` kept rows and `n_buffers` listed
/// buffers is due for compaction, given its `watermark`: the list length
/// right after its previous attempt (0 before the first).
///
/// An attempt scans the kept rows, so the list must have at least doubled
/// since the previous attempt (a frame referencing `W` buffers is rescanned
/// only after `W` new entries) and grown by one entry per
/// `ROWS_PER_NEW_ENTRY` kept rows (the scan is charged to the entries added).
/// A list that stops growing is not re-examined, even if its rows no longer
/// reference its buffers.
fn should_compact(n_rows: usize, n_buffers: usize, watermark: usize) -> bool {
    n_buffers >= (2 * watermark).max(watermark + n_rows / ROWS_PER_NEW_ENTRY + 1)
}

/// Returns `batch` without its first `n_prune` rows, like `batch.slice`, but
/// with each due view column listing only the data buffers its kept views
/// reference. `view_columns` are the batch's view columns
/// ([`view_column_indices`]) and `watermarks` their bookkeeping, updated here.
/// With nothing kept, a view column that lists buffers becomes an empty array.
/// With no view column due, this is exactly the plain slice.
pub(super) fn prune_batch_prefix(
    batch: &RecordBatch,
    n_prune: usize,
    view_columns: &[usize],
    watermarks: &mut [usize],
) -> Result<RecordBatch> {
    debug_assert_eq!(view_columns.len(), watermarks.len());
    let n_keep = batch.num_rows() - n_prune;
    let kept = batch.slice(n_prune, n_keep);
    if n_keep == 0 {
        watermarks.fill(0);
    }
    let due: Vec<usize> = (0..view_columns.len())
        .filter(|&i| {
            let n_buffers = view_buffer_count(batch.column(view_columns[i]));
            // A zero-row slice still lists every buffer.
            if n_keep == 0 {
                n_buffers > 0
            } else {
                should_compact(n_keep, n_buffers, watermarks[i])
            }
        })
        .collect();
    if due.is_empty() {
        return Ok(kept);
    }

    let mut columns = kept.columns().to_vec();
    // Columns that are the same array in `batch` are rewritten once and stay
    // shared.
    let mut rewritten: HashMap<*const (), ArrayRef> = HashMap::with_capacity(due.len());
    for i in due {
        let column = view_columns[i];
        let compacted = rewritten
            .entry(Arc::as_ptr(batch.column(column)).cast::<()>())
            .or_insert_with(|| {
                if n_keep == 0 {
                    new_empty_array(columns[column].data_type())
                } else {
                    compact_view_column(&columns[column])
                        .unwrap_or_else(|| Arc::clone(&columns[column]))
                }
            });
        watermarks[i] = view_buffer_count(compacted);
        columns[column] = Arc::clone(compacted);
    }
    Ok(RecordBatch::try_new_with_options(
        kept.schema(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(n_keep)),
    )?)
}

/// [`prune_batch_prefix`] for one array, such as the results a window
/// expression keeps for a partition; `watermark` is its bookkeeping.
pub(super) fn prune_array_prefix(
    array: &ArrayRef,
    n_prune: usize,
    watermark: &mut usize,
) -> ArrayRef {
    let n_keep = array.len() - n_prune;
    let kept = array.slice(n_prune, n_keep);
    let n_buffers = view_buffer_count(&kept);
    if n_keep == 0 {
        *watermark = 0;
        return if n_buffers > 0 {
            new_empty_array(array.data_type())
        } else {
            kept
        };
    }
    if !should_compact(n_keep, n_buffers, *watermark) {
        return kept;
    }
    let pruned = compact_view_column(&kept).unwrap_or(kept);
    *watermark = view_buffer_count(&pruned);
    pruned
}

/// The number of data buffers a view column lists; 0 for other types.
fn view_buffer_count(column: &ArrayRef) -> usize {
    match column.data_type() {
        DataType::Utf8View => column.as_string_view().data_buffers().len(),
        DataType::BinaryView => column.as_binary_view().data_buffers().len(),
        _ => 0,
    }
}

/// `column` listing only the buffers its views reference, or `None` if it
/// already does (see [`referenced_buffers`]).
fn compact_view_column(column: &ArrayRef) -> Option<ArrayRef> {
    match column.data_type() {
        DataType::Utf8View => referenced_buffers(column.as_string_view())
            .map(|array| Arc::new(array) as ArrayRef),
        DataType::BinaryView => referenced_buffers(column.as_binary_view())
            .map(|array| Arc::new(array) as ArrayRef),
        _ => None,
    }
}

/// Returns `array` listing only the data buffers its views reference, each
/// once, or `None` if it already does. Entries are the same buffer when they
/// have the same pointer and length. The kept entries are the original
/// `Buffer`s: no bytes are copied, only the views are rewritten.
fn referenced_buffers<T: ByteViewType>(
    array: &GenericByteViewArray<T>,
) -> Option<GenericByteViewArray<T>> {
    const UNREFERENCED: u32 = u32::MAX;
    let buffers = array.data_buffers();
    let nulls = array.nulls();
    let is_referenced = |index: usize, raw: &u128| {
        (*raw as u32) > MAX_INLINE_VIEW_LEN && !nulls.is_some_and(|n| n.is_null(index))
    };

    // The kept buffers, in order of first reference, and the new index of
    // every referenced entry.
    let mut remap = vec![UNREFERENCED; buffers.len()];
    let mut kept: Vec<Buffer> = Vec::new();
    let mut first_index: HashMap<(*const u8, usize), u32> = HashMap::new();
    for (index, raw) in array.views().iter().enumerate() {
        if !is_referenced(index, raw) {
            continue;
        }
        let entry = &mut remap[ByteView::from(*raw).buffer_index as usize];
        if *entry == UNREFERENCED {
            let buffer = &buffers[ByteView::from(*raw).buffer_index as usize];
            *entry = *first_index
                .entry((buffer.as_ptr(), buffer.len()))
                .or_insert_with(|| {
                    kept.push(buffer.clone());
                    (kept.len() - 1) as u32
                });
        }
    }
    if kept.len() == buffers.len() {
        return None;
    }

    // Null views may point at a dropped entry, so they are zeroed.
    let views: Vec<u128> = array
        .views()
        .iter()
        .enumerate()
        .map(|(index, raw)| {
            if !is_referenced(index, raw) {
                return if (*raw as u32) <= MAX_INLINE_VIEW_LEN {
                    *raw
                } else {
                    0
                };
            }
            let view = ByteView::from(*raw);
            ByteView {
                buffer_index: remap[view.buffer_index as usize],
                ..view
            }
            .as_u128()
        })
        .collect();
    let views = ScalarBuffer::from(views);
    let buffers: Arc<[Buffer]> = kept.into();

    #[cfg(debug_assertions)]
    T::validate(&views, &buffers).expect("remapped views stay valid");

    // SAFETY: every non-null, non-inline view keeps its offset and length and
    // points at an entry of `buffers` that holds the same bytes as the entry
    // it pointed at in `array` (same pointer and length); inline views are
    // unchanged and null views are inline empty views. The null buffer is the
    // array's own, of the same length as the views.
    Some(unsafe { GenericByteViewArray::new_unchecked(views, buffers, nulls.cloned()) })
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{BinaryViewArray, Int32Array, StringViewArray, StringViewBuilder};
    use arrow::buffer::NullBuffer;
    use arrow::compute::concat;

    fn long(i: usize) -> String {
        format!("a value longer than twelve bytes, number {i:06}")
    }

    /// `values.len()` rows listing as many data buffers.
    fn one_buffer_per_row(values: std::ops::Range<usize>) -> ArrayRef {
        let parts: Vec<_> = values
            .map(|i| StringViewArray::from(vec![long(i)]))
            .collect();
        concat(&parts.iter().map(|a| a as &dyn Array).collect::<Vec<_>>()).unwrap()
    }

    fn batch_of(columns: Vec<ArrayRef>) -> RecordBatch {
        let named = columns
            .into_iter()
            .enumerate()
            .map(|(i, c)| (format!("c{i}"), c));
        RecordBatch::try_from_iter(named).unwrap()
    }

    fn n_buffers(batch: &RecordBatch, column: usize) -> usize {
        view_buffer_count(batch.column(column))
    }

    fn strings(batch: &RecordBatch, column: usize) -> Vec<Option<String>> {
        let s = batch.column(column).as_string_view();
        s.iter().map(|v| v.map(str::to_string)).collect()
    }

    /// Nulls (one a long view masked null), inline values, an input slice
    /// with a non-zero offset, a `BinaryView` column and two columns that are
    /// the same array; then nothing kept; then only inline and null rows kept.
    #[test]
    fn remap_keeps_values_nulls_and_shared_columns() -> Result<()> {
        let mixed: StringViewArray = vec![Some("inline"), None, Some(long(6).as_str())]
            .into_iter()
            .collect();
        let all = concat(&[one_buffer_per_row(0..6).as_ref(), &mixed])?;
        let s = all.as_string_view();
        // Row 4, a long view into buffer 4, masked null.
        let nulls = NullBuffer::from_iter((0..9).map(|i| i != 4));
        let masked = StringViewArray::new(
            s.views().clone(),
            s.data_buffers().to_vec(),
            Some(nulls),
        );
        let masked: ArrayRef = Arc::new(masked.slice(1, 8));
        let binary: BinaryViewArray = (0..9).map(|i| Some(long(10 + i))).collect();
        let binary = concat(&[&binary.slice(1, 3), &binary.slice(4, 5)])?;
        let b = batch_of(vec![Arc::clone(&masked), masked, binary]);
        assert_eq!((n_buffers(&b, 0), n_buffers(&b, 2)), (7, 2));

        // Keep rows 3.. of the slice: masked null, 5, inline, null, 6.
        let mut watermarks = [0; 3];
        let pruned = prune_batch_prefix(&b, 3, &[0, 1, 2], &mut watermarks)?;
        assert_eq!(strings(&pruned, 0), strings(&b.slice(3, 5), 0));
        assert_eq!(pruned.column(0).as_string_view().views()[0], 0);
        assert!(Arc::ptr_eq(pruned.column(0), pruned.column(1)));
        assert_eq!(
            pruned.column(2).as_binary_view().value(4),
            long(18).as_bytes()
        );
        assert_eq!(watermarks, [2, 2, 1]);

        let emptied = prune_batch_prefix(&pruned, 5, &[0, 1, 2], &mut watermarks)?;
        assert_eq!((n_buffers(&emptied, 0), n_buffers(&emptied, 2)), (0, 0));
        assert!(Arc::ptr_eq(emptied.column(0), emptied.column(1)));
        assert_eq!(watermarks, [0; 3]);

        let short = concat(&[one_buffer_per_row(0..2).as_ref(), &mixed.slice(0, 2)])?;
        let mut watermarks = [1];
        let pruned =
            prune_batch_prefix(&batch_of(vec![short]), 2, &[0], &mut watermarks)?;
        assert_eq!(strings(&pruned, 0), [Some("inline".to_string()), None]);
        assert_eq!((pruned.num_rows(), n_buffers(&pruned, 0)), (2, 0));
        assert_eq!(watermarks, [0]);
        Ok(())
    }

    /// Repeated entries of one buffer are merged without copying it; entries
    /// exposing different ranges of one allocation stay separate.
    #[test]
    fn entries_are_identified_by_pointer_and_length() -> Result<()> {
        let repeated = Buffer::from_vec(vec![b'x'; 1024]);
        let allocation = Buffer::from_vec(vec![b'y'; 64]);
        let mut builder = StringViewBuilder::new();
        for _ in 0..100 {
            let block = builder.append_block(repeated.clone());
            builder.try_append_view(block, 0, 1024)?;
        }
        for offset in [0, 32] {
            let block = builder.append_block(allocation.slice_with_length(offset, 32));
            builder.try_append_view(block, 0, 32)?;
        }
        let b = batch_of(vec![Arc::new(builder.finish())]);

        let mut watermarks = [0];
        let pruned = prune_batch_prefix(&b, 1, &[0], &mut watermarks)?;
        let s = pruned.column(0).as_string_view();
        assert_eq!(s.data_buffers().len(), 3);
        assert_eq!(s.data_buffers()[0].as_ptr(), repeated.as_ptr());
        assert_eq!(
            (s.value(98).len(), s.value(100)),
            (1024, "y".repeat(32).as_str())
        );
        Ok(())
    }

    /// Without view columns, or with none due, the result is the plain slice.
    #[test]
    fn no_view_column_due_is_a_plain_slice() -> Result<()> {
        let numbers = batch_of(vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]);
        for n_prune in 0..=3 {
            let pruned = prune_batch_prefix(&numbers, n_prune, &[], &mut [])?;
            assert_eq!(pruned, numbers.slice(n_prune, 3 - n_prune));
        }
        // Four buffers, none referenced by the kept row, but the list has
        // not doubled since an attempt that left three.
        let b = batch_of(vec![one_buffer_per_row(0..4)]);
        let mut watermarks = [3];
        let pruned = prune_batch_prefix(&b, 3, &[0], &mut watermarks)?;
        assert_eq!(pruned, b.slice(3, 1));
        // The same views and data buffers, not copies.
        let ptrs = |b: &RecordBatch| {
            let s = b.column(0).as_string_view();
            let buffers: Vec<_> = s.data_buffers().iter().map(|b| b.as_ptr()).collect();
            (s.views().as_ptr(), buffers)
        };
        assert_eq!(ptrs(&pruned), ptrs(&b.slice(3, 1)));
        assert_eq!((n_buffers(&pruned, 0), watermarks[0]), (4, 3));
        Ok(())
    }

    /// The operator's loop over a frame spanning 100 batches: `s` keeps 100
    /// live buffers and is rescanned only once its list has doubled; `t`
    /// lists one buffer once per batch and is compacted on every prune,
    /// independently of `s`.
    #[test]
    fn attempts_are_amortized_per_column() -> Result<()> {
        assert!(!should_compact(1, 0, 0) && should_compact(1, 1, 0));
        assert!(!should_compact(1_000_000, 244, 0));
        assert!(!should_compact(1_000_000, 366, 122));
        assert!(should_compact(1_000_000, 367, 122));

        let t_all: ArrayRef = Arc::new(StringViewArray::from_iter_values(
            (0..200).map(|i| long(1000 + i)),
        ));
        let rows = |range: std::ops::Range<usize>| {
            let t = t_all.slice(range.start, range.len());
            batch_of(vec![one_buffer_per_row(range), t])
        };
        let mut kept = rows(0..100);
        let mut watermarks = [0, 0];
        kept = prune_batch_prefix(&kept, 0, &[0, 1], &mut watermarks)?;
        assert_eq!(watermarks, [100, 1]);
        for i in 100..200 {
            let extended =
                arrow::compute::concat_batches(&kept.schema(), [&kept, &rows(i..i + 1)])?;
            kept = prune_batch_prefix(&extended, 1, &[0, 1], &mut watermarks)?;
            // Untouched until the 100th addition doubles the list.
            let expected = if i < 199 { i + 1 } else { 100 };
            assert_eq!(n_buffers(&kept, 0), expected, "after batch {i}");
            assert_eq!((n_buffers(&kept, 1), watermarks), (1, [100, 1]));
        }
        let expected: Vec<_> = (100..200).map(|i| Some(long(i))).collect();
        assert_eq!(strings(&kept, 0), expected);
        Ok(())
    }
}
