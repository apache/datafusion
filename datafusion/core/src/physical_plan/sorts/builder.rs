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

use datafusion_common::Result;
use std::collections::VecDeque;
use std::mem::take;

use super::cursor::Cursor;
use super::stream::{BatchId, BatchOffset};

pub type SortOrder = (BatchId, usize, BatchOffset); // batch_id, row_idx (without offset)

#[derive(Debug)]
struct BatchCursor<C: Cursor> {
    /// The index into BatchTrackingStream::batches
    batch: BatchId,
    /// The row index within the given batch
    row_idx: usize,
    /// The offset of the row within the given batch, based on sliced cursors
    row_offset: BatchOffset,
    /// The cursor for the given batch. If None, the batch is finished.
    cursor: C,
}

/// Provides an API to incrementally build a [`SortOrder`] from partitioned [`RecordBatch`](arrow::record_batch::RecordBatch)es
#[derive(Debug)]
pub struct SortOrderBuilder<C: Cursor> {
    /// Maintain a list of cursors for each finished (sorted) batch
    /// The number of total batches can be larger than the number of total streams
    batch_cursors: VecDeque<BatchCursor<C>>,

    /// The current [`BatchCursor`] for each stream_idx
    cursors: Vec<Option<BatchCursor<C>>>,

    /// The accumulated stream indexes from which to pull rows
    indices: Vec<SortOrder>,

    stream_count: usize,
}

impl<C: Cursor> SortOrderBuilder<C> {
    /// Create a new [`SortOrderBuilder`] with the provided `stream_count` and `batch_size`
    pub fn new(stream_count: usize, batch_size: usize) -> Self {
        Self {
            batch_cursors: VecDeque::with_capacity(stream_count * 2),
            cursors: (0..stream_count).map(|_| None).collect(),
            indices: Vec::with_capacity(batch_size),
            stream_count,
        }
    }

    /// Append a new batch in `stream_idx`
    pub fn push_batch(
        &mut self,
        stream_idx: usize,
        mut cursor: C,
        batch: BatchId,
        row_offset: BatchOffset,
    ) -> Result<()> {
        cursor.seek(0);
        self.cursors[stream_idx] = Some(BatchCursor {
            batch,
            row_idx: 0,
            row_offset,
            cursor,
        });
        Ok(())
    }

    /// Append the next row from `stream_idx`
    pub fn push_row(&mut self, stream_idx: usize) {
        if let Some(batch_cursor) = &mut self.cursors[stream_idx] {
            let row_idx = batch_cursor.row_idx;
            self.indices
                .push((batch_cursor.batch, row_idx, batch_cursor.row_offset));
            if batch_cursor.cursor.is_finished() {
                self.cursor_finished(stream_idx);
            } else {
                batch_cursor.row_idx += 1;
            }
        } else {
            unreachable!("row pushed for non-existant cursor");
        };
    }

    /// Returns the number of in-progress rows in this [`SortOrderBuilder`]
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    /// Returns `true` if this [`SortOrderBuilder`] contains no in-progress rows
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    /// For a finished cursor, remove from BatchCursor (per stream_idx), and track in batch_cursors (per batch_idx)
    fn cursor_finished(&mut self, stream_idx: usize) {
        if let Some(cursor) = self.cursors[stream_idx].take() {
            self.batch_cursors.push_back(cursor);
        }
    }

    /// Advance the cursor for `stream_idx`
    /// Returns `true` if the cursor was advanced
    pub fn advance(&mut self, stream_idx: usize) -> bool {
        match &mut self.cursors[stream_idx] {
            Some(batch_cursor) => {
                let cursor = &mut batch_cursor.cursor;
                if cursor.is_finished() {
                    return false;
                }
                cursor.advance();
                true
            }
            None => false,
        }
    }

    /// Returns true if there is an in-progress cursor for a given stream
    pub fn cursor_in_progress(&mut self, stream_idx: usize) -> bool {
        self.cursors[stream_idx]
            .as_mut()
            .map_or(false, |batch_cursor| !batch_cursor.cursor.is_finished())
    }

    /// Returns `true` if the cursor at index `a` is greater than at index `b`
    #[inline]
    pub fn is_gt(&mut self, stream_idx_a: usize, stream_idx_b: usize) -> bool {
        match (
            self.cursor_in_progress(stream_idx_a),
            self.cursor_in_progress(stream_idx_b),
        ) {
            (false, _) => true,
            (_, false) => false,
            _ => match (&self.cursors[stream_idx_a], &self.cursors[stream_idx_b]) {
                (Some(a), Some(b)) => a
                    .cursor
                    .cmp(&b.cursor)
                    .then_with(|| stream_idx_a.cmp(&stream_idx_b))
                    .is_gt(),
                _ => unreachable!(),
            },
        }
    }

    /// Takes the batches which already are sorted, and returns them with the corresponding cursors and sort order.
    ///
    /// This will drain the internal state of the builder, and return `None` if there are no pending.
    ///
    /// This slices cursors for each record batch, as follows:
    /// 1. input was N record_batchs of up to max M size
    /// 2. yielded ordered rows can only equal up to M size
    /// 3. of the N record_batches, each will be:
    ///         a. fully yielded (all rows)
    ///         b. partially yielded (some rows) => slice cursor, and adjust BatchOffset
    ///         c. not yielded (no rows) => retain cursor
    /// 4. output will be:
    ///        - SortOrder
    ///        - corresponding cursors, each up to total yielded rows [cursor_batch_0, cursor_batch_1, ..]
    pub fn yield_sort_order(
        &mut self,
    ) -> Result<Option<(Vec<(C, BatchId, BatchOffset)>, Vec<SortOrder>)>> {
        if self.is_empty() {
            return Ok(None);
        }

        let sort_order = take(&mut self.indices);
        let mut cursors_to_yield: Vec<(C, BatchId, BatchOffset)> =
            Vec::with_capacity(self.stream_count * 2);

        // drain already complete cursors
        for _ in 0..self.batch_cursors.len() {
            let BatchCursor {
                batch,
                row_idx: _,
                row_offset,
                cursor: mut row_cursor,
            } = self.batch_cursors.pop_front().expect("must have a cursor");
            row_cursor.seek(0);
            cursors_to_yield.push((row_cursor, batch, row_offset));
        }

        // split any in_progress cursor
        for stream_idx in 0..self.cursors.len() {
            let batch_cursor = match self.cursors[stream_idx].take() {
                Some(c) => c,
                None => continue,
            };
            let BatchCursor {
                batch,
                row_idx,
                row_offset,
                cursor: row_cursor,
            } = batch_cursor;

            let is_fully_yielded = row_idx == row_cursor.num_rows();
            let to_split = row_idx > 0 && !is_fully_yielded;

            if is_fully_yielded {
                cursors_to_yield.push((row_cursor, batch, row_offset));
            } else if to_split {
                let row_cursor_to_yield = row_cursor.slice(0, row_idx)?;
                let row_cursor_to_retain =
                    row_cursor.slice(row_idx, row_cursor.num_rows() - row_idx)?;
                assert_eq!(
                    row_cursor_to_yield.num_rows() + row_cursor_to_retain.num_rows(),
                    row_cursor.num_rows()
                );
                drop(row_cursor); // drop the original cursor

                self.cursors[stream_idx] = Some(BatchCursor {
                    batch,
                    row_idx: 0,
                    row_offset: BatchOffset(row_offset.0 + row_idx),
                    cursor: row_cursor_to_retain,
                });
                cursors_to_yield.push((row_cursor_to_yield, batch, row_offset));
            } else {
                // retained all (nothing yielded)
                self.cursors[stream_idx] = Some(BatchCursor {
                    batch,
                    row_idx,
                    row_offset,
                    cursor: row_cursor,
                });
            }
        }

        Ok(Some((cursors_to_yield, sort_order)))
    }
}
