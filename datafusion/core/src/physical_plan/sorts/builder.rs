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

use crate::common::Result;
use crate::physical_plan::sorts::index::RowIndex;
use arrow::array::{make_array, MutableArrayData};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use std::collections::VecDeque;

/// Provides an API to incrementally build a [`RecordBatch`] from partitioned [`RecordBatch`]
#[derive(Debug)]
pub struct BatchBuilder {
    /// The schema of the RecordBatches yielded by this stream
    schema: SchemaRef,
    /// For each input stream maintain a dequeue of RecordBatches
    ///
    /// Exhausted batches will be popped off the front once all
    /// their rows have been yielded to the output
    batches: Vec<VecDeque<RecordBatch>>,

    /// The accumulated row indexes for the next record batch
    indices: Vec<RowIndex>,
}

impl BatchBuilder {
    /// Create a new [`BatchBuilder`] with the provided `stream_count` and `batch_size`
    pub fn new(schema: SchemaRef, stream_count: usize, batch_size: usize) -> Self {
        let batches = (0..stream_count).map(|_| VecDeque::new()).collect();

        Self {
            schema,
            batches,
            indices: Vec::with_capacity(batch_size),
        }
    }

    /// Append a new batch in `stream_idx`
    pub fn push_batch(&mut self, stream_idx: usize, batch: RecordBatch) {
        self.batches[stream_idx].push_back(batch)
    }

    /// Push `row_idx` from the most recently appended batch in `stream_idx`
    pub fn push_row(&mut self, stream_idx: usize, row_idx: usize) {
        let batch_idx = self.batches[stream_idx].len() - 1;
        self.indices.push(RowIndex {
            stream_idx,
            batch_idx,
            row_idx,
        });
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

    /// Drains the in_progress row indexes, and builds a new RecordBatch from them
    ///
    /// Will then drop any batches for which all rows have been yielded to the output
    ///
    /// Returns `None` if no pending rows
    pub fn build_record_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.is_empty() {
            return Ok(None);
        }

        // Mapping from stream index to the index of the first buffer from that stream
        let mut buffer_idx = 0;
        let mut stream_to_buffer_idx = Vec::with_capacity(self.batches.len());

        for batches in &self.batches {
            stream_to_buffer_idx.push(buffer_idx);
            buffer_idx += batches.len();
        }

        let columns = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(column_idx, field)| {
                let arrays = self
                    .batches
                    .iter()
                    .flat_map(|batch| {
                        batch.iter().map(|batch| batch.column(column_idx).data())
                    })
                    .collect();

                let mut array_data = MutableArrayData::new(
                    arrays,
                    field.is_nullable(),
                    self.indices.len(),
                );

                let first = &self.indices[0];
                let mut buffer_idx =
                    stream_to_buffer_idx[first.stream_idx] + first.batch_idx;
                let mut start_row_idx = first.row_idx;
                let mut end_row_idx = start_row_idx + 1;

                for row_index in self.indices.iter().skip(1) {
                    let next_buffer_idx =
                        stream_to_buffer_idx[row_index.stream_idx] + row_index.batch_idx;

                    if next_buffer_idx == buffer_idx && row_index.row_idx == end_row_idx {
                        // subsequent row in same batch
                        end_row_idx += 1;
                        continue;
                    }

                    // emit current batch of rows for current buffer
                    array_data.extend(buffer_idx, start_row_idx, end_row_idx);

                    // start new batch of rows
                    buffer_idx = next_buffer_idx;
                    start_row_idx = row_index.row_idx;
                    end_row_idx = start_row_idx + 1;
                }

                // emit final batch of rows
                array_data.extend(buffer_idx, start_row_idx, end_row_idx);
                make_array(array_data.freeze())
            })
            .collect();

        self.indices.clear();

        // New cursors are only created once the previous cursor for the stream
        // is finished. This means all remaining rows from all but the last batch
        // for each stream have been yielded to the newly created record batch
        //
        // Additionally as `in_progress` has been drained, there are no longer
        // any RowIndex's reliant on the batch indexes
        //
        // We can therefore drop all but the last batch for each stream
        for batches in &mut self.batches {
            if batches.len() > 1 {
                // Drain all but the last batch
                batches.drain(0..(batches.len() - 1));
            }
        }

        Ok(Some(RecordBatch::try_new(self.schema.clone(), columns)?))
    }
}
