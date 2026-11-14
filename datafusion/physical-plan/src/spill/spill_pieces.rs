// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Splits a batch into byte-bounded pieces before it is written to a spill file.

use std::ops::Range;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Schema};
use arrow_data::ArrayData;
use datafusion_common::Result;

use super::compact::compact_batch;
use super::spill_manager::GetSlicedSize;
use super::spilled_row_sizes::SpilledRowSizes;
use super::{gc_view_arrays, get_max_alignment_for_schema};

/// One piece of a batch, to be written as one IPC message.
#[derive(Debug)]
pub(crate) struct SpillPiece {
    pub(crate) batch: RecordBatch,
    /// The size recorded for the piece, see [`recorded_size`].
    pub(crate) size: usize,
}

/// The pieces that one appended batch is written as, in row order.
///
/// Without a bound, there is one piece: the batch after [`gc_view_arrays`], with its
/// [`GetSlicedSize`]. When the batch is within the bound, there is one piece too, with its
/// [`recorded_size`].
///
/// Otherwise the batch is cut into consecutive row ranges by the size of each row once
/// spilled ([`SpilledRowSizes`]), less the padding that every piece adds. Each range is
/// copied into fresh buffers that hold only its rows (see [`compact_batch`]), so the
/// [`recorded_size`] of a piece is what it holds, whatever its type. A piece that is still
/// larger than the bound is halved until it fits or has one row.
/// Pieces are produced one at a time, so that the caller can write each one before the
/// next is copied.
///
/// Data that rows share is not copied into more pieces than needed:
///
/// | Shared data                        | Handling                                         |
/// |------------------------------------|--------------------------------------------------|
/// | Dictionaries that are not compacted | Every piece keeps all their values. The file and the reader hold them once. A piece can exceed the bound by their size. |
/// | Bytes that several views reference  | A piece copies them once. Only the first view of a value counts it in [`SpilledRowSizes`]. When more than half of the view bytes are repeats, the batch is written whole. |
/// | Child ranges of list views          | A piece copies the range that its rows cover once. |
///
/// A piece whose rows share data does not get smaller when halved. When the first half of
/// a range is not clearly smaller than the range, the range is written whole, above the
/// bound, since halving it would only copy the shared data into more pieces.
///
/// Temporary workaround: the cuts by [`SpilledRowSizes`], the halving retry and
/// [`compact_batch`] compensate for sizes and copies that arrow-rs does not provide yet. The arrow-rs `minify`,
/// `compacted_size` and `row_sizes` APIs (<https://github.com/apache/arrow-rs/pull/12184>)
/// and the IPC option to write only the used view bytes
/// (<https://github.com/apache/arrow-rs/pull/12039>) replace them.
pub(crate) struct SpillPieces {
    batch: RecordBatch,
    max_batch_bytes: usize,
    compressed: bool,
    /// The bytes left for rows in each piece, once its padding is counted.
    row_budget: usize,
    /// `None` when the batch is written whole.
    row_sizes: Option<SpilledRowSizes>,
    /// For each column, whether its dictionaries keep all their values in every piece.
    keep_dictionaries: Vec<bool>,
    /// The bytes of the dictionary values that every piece keeps. They do not count
    /// against the bound.
    shared: usize,
    /// The first row not yet in a piece.
    next_row: usize,
    /// Halves of ranges that were too large, the next one last.
    pending: Vec<Half>,
    /// The whole batch, when it is written as one piece.
    whole: Option<SpillPiece>,
}

impl SpillPieces {
    pub(crate) fn new(
        batch: &RecordBatch,
        max_batch_bytes: Option<usize>,
        compressed: bool,
    ) -> Result<Self> {
        let batch = gc_view_arrays(batch)?;
        let Some(max_batch_bytes) = max_batch_bytes else {
            // Without a bound, record the size as an unbounded writer always has.
            let size = batch.get_sliced_size()?;
            return Ok(Self::whole(batch, size, compressed));
        };
        let row_sizes = SpilledRowSizes::new_with_shared_views_once(&batch);
        let keep_dictionaries = (0..batch.num_columns())
            .map(|column| !row_sizes.compacted_columns().contains(&column))
            .collect::<Vec<_>>();
        let shared = batch
            .columns()
            .iter()
            .zip(&keep_dictionaries)
            .filter(|(_, keep)| **keep)
            .map(|(column, _)| dictionary_values_size(&column.to_data()))
            .sum::<Result<usize>>()?;
        let size = recorded_size(&batch, compressed)?;
        // The pieces hold at most the bytes that `SpilledRowSizes::new` counts, which
        // copies the bytes of each view. When views share most of those bytes, the
        // pieces could hold many copies of them, so the batch is written whole.
        let rows = batch.num_rows();
        let shared_views =
            SpilledRowSizes::new(&batch).rows(0, rows) > 2 * row_sizes.rows(0, rows);
        if size.saturating_sub(shared) <= max_batch_bytes || rows <= 1 || shared_views {
            return Ok(Self::whole(batch, size, compressed));
        }
        let padding = padding(&batch, padding_alignment(batch.schema_ref(), compressed));
        Ok(Self {
            row_sizes: Some(row_sizes),
            keep_dictionaries,
            shared,
            batch,
            max_batch_bytes,
            compressed,
            row_budget: max_batch_bytes.saturating_sub(padding),
            next_row: 0,
            pending: vec![],
            whole: None,
        })
    }

    fn whole(batch: RecordBatch, size: usize, compressed: bool) -> Self {
        Self {
            whole: Some(SpillPiece {
                batch: batch.clone(),
                size,
            }),
            batch,
            max_batch_bytes: 0,
            compressed,
            row_budget: 0,
            row_sizes: None,
            keep_dictionaries: vec![],
            shared: 0,
            next_row: usize::MAX,
            pending: vec![],
        }
    }

    /// Returns the next range of rows, from `self.next_row`, that fits the row budget.
    fn next_range(&mut self, row_sizes: &SpilledRowSizes) -> Option<Range<usize>> {
        let (start, rows) = (self.next_row, self.batch.num_rows());
        if start >= rows {
            return None;
        }
        let end = if let Some(row_bytes) = row_sizes.fixed() {
            start + (self.row_budget / row_bytes.max(1)).clamp(1, rows - start)
        } else {
            let mut bytes = 0;
            let mut end = start;
            while end < rows {
                bytes += row_sizes.row(end);
                if end > start && bytes > self.row_budget {
                    break;
                }
                end += 1;
            }
            end
        };
        self.next_row = end;
        Some(start..end)
    }

    /// Copies the rows of `range` into a piece, and returns it with its
    /// [`recorded_size`].
    fn compact(&self, range: &Range<usize>) -> Result<(RecordBatch, usize)> {
        let piece = self.batch.slice(range.start, range.len());
        let piece = compact_batch(&piece, &self.keep_dictionaries)?;
        let size = recorded_size(&piece, self.compressed)?;
        Ok((piece, size))
    }
}

/// One half of a range that was larger than the bound.
struct Half {
    range: Range<usize>,
    /// The range it was cut from.
    parent: Range<usize>,
    /// The bytes of the parent piece that count against the bound.
    parent_own: usize,
    /// Whether this is the first half. The second half is next on the stack.
    first: bool,
}

/// The memory size of the dictionary values in `data`, at any nesting level.
fn dictionary_values_size(data: &ArrayData) -> Result<usize> {
    if let DataType::Dictionary(_, _) = data.data_type() {
        return Ok(data.child_data()[0].get_slice_memory_size()?);
    }
    data.child_data().iter().map(dictionary_values_size).sum()
}

impl Iterator for SpillPieces {
    type Item = Result<SpillPiece>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(whole) = self.whole.take() {
            return Some(Ok(whole));
        }
        let row_sizes = self.row_sizes.take()?;
        let piece = loop {
            let (range, half) = match self.pending.pop() {
                Some(half) => (half.range.clone(), Some(half)),
                None => match self.next_range(&row_sizes) {
                    Some(range) => (range, None),
                    None => break None,
                },
            };
            let (piece, size) = match self.compact(&range) {
                Ok(piece) => piece,
                Err(e) => break Some(Err(e)),
            };
            // The bytes of the piece that count against the bound.
            let own = size.saturating_sub(self.shared);
            if let Some(Half {
                parent,
                parent_own,
                first: true,
                ..
            }) = half
                && own >= parent_own / 4 * 3
            {
                // The halves share most of their data. Write the parent instead.
                self.pending.pop();
                break Some(
                    self.compact(&parent)
                        .map(|(batch, size)| SpillPiece { batch, size }),
                );
            }
            if own > self.max_batch_bytes && range.len() > 1 {
                let mid = range.start + range.len() / 2;
                let half = |half: Range<usize>, first| Half {
                    range: half,
                    parent: range.clone(),
                    parent_own: own,
                    first,
                };
                self.pending.push(half(mid..range.end, false));
                self.pending.push(half(range.start..mid, true));
                continue;
            }
            break Some(Ok(SpillPiece { batch: piece, size }));
        };
        self.row_sizes = Some(row_sizes);
        piece
    }
}

/// The capacity granularity of arrow's `MutableBuffer`, which a compressed buffer is
/// decompressed into.
const DECOMPRESSED_ALIGNMENT: usize = 64;

/// The size recorded for a batch written to a spill file: an upper bound on the memory a
/// reader uses for it once decoded from IPC. Use it wherever a batch read back is compared
/// with the recorded size.
///
/// It is the batch's [`GetSlicedSize`] plus the padding of every buffer. Without
/// compression the buffers are slices of the message body, where the IPC writer pads each
/// one to the schema's alignment. With compression each is decompressed into its own
/// allocation, rounded up to [`DECOMPRESSED_ALIGNMENT`]. For many narrow columns the
/// padding can be larger than the data.
pub(crate) fn recorded_size(batch: &RecordBatch, compressed: bool) -> Result<usize> {
    let alignment = padding_alignment(batch.schema_ref(), compressed);
    Ok(batch.get_sliced_size()? + padding(batch, alignment))
}

/// The padding a reader can add to each buffer of `schema`.
fn padding_alignment(schema: &Schema, compressed: bool) -> usize {
    let alignment = get_max_alignment_for_schema(schema);
    if compressed {
        alignment.max(DECOMPRESSED_ALIGNMENT)
    } else {
        alignment
    }
}

/// The padding of every buffer of `batch`, at `alignment` bytes each.
fn padding(batch: &RecordBatch, alignment: usize) -> usize {
    fn buffers(data: &ArrayData) -> usize {
        // One validity buffer, which the writer emits even when it is empty.
        1 + data.buffers().len() + data.child_data().iter().map(buffers).sum::<usize>()
    }
    let buffers: usize = batch.columns().iter().map(|c| buffers(&c.to_data())).sum();
    buffers * alignment
}
