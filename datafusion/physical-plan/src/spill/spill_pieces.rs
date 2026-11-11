// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Splits a batch into byte-bounded pieces before it is written to a spill file.

use std::ops::Range;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
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
/// larger than the bound is halved until it fits, has one row, or stops getting smaller.
/// Pieces are produced one at a time, so that the caller can write each one before the
/// next is copied.
///
/// A piece keeps only the dictionary values that its keys use, so a dictionary that the
/// rows of a batch share is written once per piece. `SpilledRowSizes` counts such a
/// dictionary by its keys only, so the first cut can be too large and is then halved.
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
    /// The first row not yet in a piece.
    next_row: usize,
    /// Halves of ranges that were too large, the next one last, each with the
    /// [`recorded_size`] of the range it was cut from.
    pending: Vec<(Range<usize>, usize)>,
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
        let size = recorded_size(&batch, compressed)?;
        if size <= max_batch_bytes || batch.num_rows() <= 1 {
            return Ok(Self::whole(batch, size, compressed));
        }
        let padding = padding(&batch, padding_alignment(batch.schema_ref(), compressed));
        Ok(Self {
            row_sizes: Some(SpilledRowSizes::new(&batch)),
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
}

impl Iterator for SpillPieces {
    type Item = Result<SpillPiece>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(whole) = self.whole.take() {
            return Some(Ok(whole));
        }
        let row_sizes = self.row_sizes.take()?;
        let piece = loop {
            let Some((range, parent_size)) = self
                .pending
                .pop()
                .map(|(range, size)| (range, Some(size)))
                .or_else(|| self.next_range(&row_sizes).map(|range| (range, None)))
            else {
                break None;
            };
            let piece = self.batch.slice(range.start, range.len());
            let piece = match compact_batch(&piece) {
                Ok(piece) => piece,
                Err(e) => break Some(Err(e)),
            };
            let size = match recorded_size(&piece, self.compressed) {
                Ok(size) => size,
                Err(e) => break Some(Err(e)),
            };
            // `SpilledRowSizes` counts only the keys of a shared dictionary, so a piece can
            // be larger than its rows. Temporary: arrow-rs `compacted_size` replaces this.
            // Rows that share data, such as list views over one child range, do not get
            // smaller when halved. Halving them further only copies the shared data into
            // more pieces, so such a piece is written above the bound.
            let shrinks = parent_size.is_none_or(|parent| size < parent / 4 * 3);
            if size > self.max_batch_bytes && range.len() > 1 && shrinks {
                let mid = range.start + range.len() / 2;
                self.pending.push((mid..range.end, size));
                self.pending.push((range.start..mid, size));
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
