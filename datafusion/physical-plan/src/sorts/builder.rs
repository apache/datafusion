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

use arrow::compute::interleave;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::memory_pool::MemoryReservation;

use super::batches::{BatchCursor, BatchId};
use super::cursor::{Cursor, CursorValues};

type YieldedSortOrder<C> = (Vec<SortOrder>, Vec<BatchCursor<C>>);
type SortOrder = (BatchId, usize); // (batch_id, row_idx)

/// Provides an API to incrementally build a [`RecordBatch`] from partitioned [`RecordBatch`]
#[derive(Debug)]
pub struct SortOrderBuilder<C: CursorValues> {
    /// The schema of the RecordBatches yielded by this stream
    schema: SchemaRef,

    /// Maintain a list of [`RecordBatch`] and their corresponding stream
    batches: Vec<(usize, RecordBatch)>,

    /// Accounts for memory used by buffered batches
    reservation: MemoryReservation,

    /// The current [`BatchCursor`] for each stream
    cursors: Vec<Option<BatchCursor<C>>>,

    /// The accumulated stream indexes from which to pull rows
    /// Consists of a tuple of `(batch_idx, row_idx)`
    indices: Vec<(usize, usize)>,
}

impl<C: CursorValues> SortOrderBuilder<C> {
    /// Create a new [`SortOrderBuilder`] with the provided `stream_count` and `batch_size`
    pub fn new(
        schema: SchemaRef,
        stream_count: usize,
        batch_size: usize,
        reservation: MemoryReservation,
    ) -> Self {
        Self {
            schema,
            batches: Vec::with_capacity(stream_count * 2),
            cursors: (0..stream_count).map(|_| None).collect(),
            indices: Vec::with_capacity(batch_size),
            reservation,
        }
    }

    /// Append a new batch in `stream_idx`
    pub fn push_batch(
        &mut self,
        stream_idx: usize,
        cursor_values: C,
        batch: RecordBatch,
    ) -> Result<()> {
        self.reservation.try_grow(batch.get_array_memory_size())?;
        let batch_idx = self.batches.len();
        self.batches.push((stream_idx, batch));
        self.cursors[stream_idx] = Some(BatchCursor {
            batch_idx,
            cursor: Cursor::new(cursor_values),
            row_idx: 0,
        });
        Ok(())
    }

    /// Append the next row from `stream_idx`
    pub fn push_row(&mut self, stream_idx: usize) {
        let BatchCursor {
            cursor: _,
            batch_idx,
            row_idx,
        } = self.cursors[stream_idx]
            .as_mut()
            .expect("row pushed for non-existant cursor");
        self.indices.push((*batch_idx, *row_idx));
        *row_idx += 1;
    }

    /// Returns true if there is an in-progress cursor for a given stream
    pub fn cursor_in_progress(&mut self, stream_idx: usize) -> bool {
        self.cursors[stream_idx]
            .as_mut()
            .map_or(false, |batch_cursor| !batch_cursor.cursor.is_finished())
    }

    /// Advance the cursor for `stream_idx`
    /// Return true if cursor was advanced
    pub fn advance_cursor(&mut self, stream_idx: usize) -> bool {
        let slot = &mut self.cursors[stream_idx];
        match slot.as_mut() {
            Some(c) => {
                if c.cursor.is_finished() {
                    return false;
                }
                c.cursor.advance();
                true
            }
            None => false,
        }
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

    /// Returns the number of in-progress rows in this [`SortOrderBuilder`]
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    /// Returns `true` if this [`SortOrderBuilder`] contains no in-progress rows
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    /// Returns the schema of this [`SortOrderBuilder`]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Yields a sort_order and the sliced [`BatchCursor`]s
    /// representing (in total) up to N batch size.
    ///
    ///         BatchCursors
    /// ┌────────────────────────┐
    /// │    Cursor     BatchId  │
    /// │ ┌──────────┐ ┌───────┐ │
    /// │ │  1..7    │ │   A   │ |  (sliced to partial batches)
    /// │ ├──────────┤ ├───────┤ │
    /// │ │  11..14  │ │   B   │ |
    /// │ └──────────┘ └───────┘ │
    /// └────────────────────────┘
    ///
    ///
    ///         SortOrder
    /// ┌─────────────────────────────┐
    /// | (B,11) (A,1) (A,2) (B,12)...|  (up to N rows)
    /// └─────────────────────────────┘
    ///
    #[allow(dead_code)]
    pub fn yield_sort_order(&mut self) -> Result<Option<YieldedSortOrder<C>>> {
        unimplemented!("to implement in future PR");
    }

    /// Drains the in_progress row indexes, and builds a new RecordBatch from them
    ///
    /// Will then drop any batches for which all rows have been yielded to the output
    ///
    /// Returns `None` if no pending rows
    pub fn build_record_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.is_empty() {
            return Ok(None);
        }

        let columns = (0..self.schema.fields.len())
            .map(|column_idx| {
                let arrays: Vec<_> = self
                    .batches
                    .iter()
                    .map(|(_, batch)| batch.column(column_idx).as_ref())
                    .collect();
                Ok(interleave(&arrays, &self.indices)?)
            })
            .collect::<Result<Vec<_>>>()?;

        self.indices.clear();

        // New cursors are only created once the previous cursor for the stream
        // is finished. This means all remaining rows from all but the last batch
        // for each stream have been yielded to the newly created record batch
        //
        // We can therefore drop all but the last batch for each stream
        let mut batch_idx = 0;
        let mut retained = 0;
        self.batches.retain(|(stream_idx, batch)| {
            let batch_cursor = self.cursors[*stream_idx]
                .as_mut()
                .expect("should have batch cursor");

            let retain = batch_cursor.batch_idx == batch_idx;
            batch_idx += 1;

            if retain {
                batch_cursor.batch_idx = retained;
                retained += 1;
            } else {
                self.reservation.shrink(batch.get_array_memory_size());
            }
            retain
        });

        Ok(Some(RecordBatch::try_new(self.schema.clone(), columns)?))
    }
}
