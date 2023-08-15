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

use super::cursor::Cursor;

#[derive(Debug, Default)]
struct BatchCursor<C: Cursor> {
    /// The index into BatchBuilder::batches
    batch_idx: usize,
    /// The row index within the given batch
    row_idx: usize,
    /// The cursor for the given batch. If None, the batch is finished.
    cursor: Option<C>,
}

/// Provides an API to incrementally build a [`RecordBatch`] from partitioned [`RecordBatch`]
#[derive(Debug)]
pub struct BatchBuilder<C: Cursor> {
    /// The schema of the RecordBatches yielded by this stream
    schema: SchemaRef,

    /// Maintain a list of [`RecordBatch`] and their corresponding stream
    batches: Vec<(usize, RecordBatch)>,

    /// Maintain a list of cursors for each finished (sorted) batch
    /// The number of total batches can be larger than the number of total streams
    batch_cursors: Vec<Option<C>>,

    /// Accounts for memory used by buffered batches
    reservation: MemoryReservation,

    /// The current [`BatchCursor`] for each stream
    cursors: Vec<BatchCursor<C>>,

    /// The accumulated stream indexes from which to pull rows
    /// Consists of a tuple of `(batch_idx, row_idx)`
    indices: Vec<(usize, usize)>,
}

impl<C: Cursor> BatchBuilder<C> {
    /// Create a new [`BatchBuilder`] with the provided `stream_count` and `batch_size`
    pub fn new(
        schema: SchemaRef,
        stream_count: usize,
        batch_size: usize,
        reservation: MemoryReservation,
    ) -> Self {
        Self {
            schema,
            batches: Vec::with_capacity(stream_count * 2),
            batch_cursors: Vec::with_capacity(stream_count * 2),
            cursors: (0..stream_count)
                .map(|_| BatchCursor {
                    batch_idx: 0,
                    row_idx: 0,
                    cursor: None,
                })
                .collect(),
            indices: Vec::with_capacity(batch_size),
            reservation,
        }
    }

    /// Append a new batch in `stream_idx`
    pub fn push_batch(
        &mut self,
        stream_idx: usize,
        batch: RecordBatch,
        cursor: C,
    ) -> Result<()> {
        self.reservation.try_grow(batch.get_array_memory_size())?;
        let batch_idx = self.batches.len();
        self.batches.push((stream_idx, batch));
        self.batch_cursors.push(None); // placehold until cursor is finished
        self.cursors[stream_idx] = BatchCursor {
            batch_idx,
            row_idx: 0,
            cursor: Some(cursor),
        };
        Ok(())
    }

    /// Append the next row from `stream_idx`
    pub fn push_row(&mut self, stream_idx: usize) {
        let cursor = &mut self.cursors[stream_idx];
        let row_idx = cursor.row_idx;
        cursor.row_idx += 1;
        self.indices.push((cursor.batch_idx, row_idx));
    }

    /// Returns the number of in-progress rows in this [`BatchBuilder`]
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    /// Returns `true` if this [`BatchBuilder`] contains no in-progress rows
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    /// Returns the schema of this [`BatchBuilder`]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// For a finished cursor, remove from BatchCursor (per stream_idx), and track in batch_cursors (per batch_idx)
    fn cursor_finished(&mut self, stream_idx: usize) {
        let batch_idx = self.cursors[stream_idx].batch_idx;
        let row_idx = self.cursors[stream_idx].row_idx;
        match std::mem::replace(
            &mut self.cursors[stream_idx],
            BatchCursor {
                batch_idx,
                row_idx,
                cursor: None,
            },
        )
        .cursor
        {
            Some(prev_batch_cursor) => {
                self.batch_cursors[batch_idx] = Some(prev_batch_cursor)
            }
            None => unreachable!("previous cursor should not be None"),
        }
    }

    /// Advance the cursor for `stream_idx`
    /// Returns `true` if the cursor was advanced
    pub fn advance(&mut self, stream_idx: usize) -> bool {
        match &mut self.cursors[stream_idx].cursor {
            Some(c) => {
                c.advance();
                if c.is_finished() {
                    self.cursor_finished(stream_idx);
                }
                true
            }
            None => false,
        }
    }

    /// Returns true if there is an in-progress cursor for a given stream
    pub fn cursor_in_progress(&self, stream_idx: usize) -> bool {
        self.cursors[stream_idx]
            .cursor
            .as_ref()
            .map(|cursor| !cursor.is_finished())
            .unwrap_or(false)
    }

    /// Returns `true` if the cursor at index `a` is greater than at index `b`
    #[inline]
    pub fn is_gt(&self, stream_idx_a: usize, stream_idx_b: usize) -> bool {
        match (
            self.cursor_in_progress(stream_idx_a),
            self.cursor_in_progress(stream_idx_b),
        ) {
            (false, _) => true,
            (_, false) => false,
            _ => {
                match (
                    &self.cursors[stream_idx_a].cursor,
                    &self.cursors[stream_idx_b].cursor,
                ) {
                    (Some(a), Some(b)) => a
                        .cmp(&b)
                        .then_with(|| stream_idx_a.cmp(&stream_idx_b))
                        .is_gt(),
                    _ => unreachable!(),
                }
            }
        }
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
            let stream_cursor = &mut self.cursors[*stream_idx];
            let retain = stream_cursor.batch_idx == batch_idx;
            batch_idx += 1;

            if retain {
                stream_cursor.batch_idx = retained;
                retained += 1;
            } else {
                self.reservation.shrink(batch.get_array_memory_size());
            }
            retain
        });

        Ok(Some(RecordBatch::try_new(self.schema.clone(), columns)?))
    }
}
