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

use arrow::row::{Row, Rows};
use std::cmp::Ordering;

/// A `SortKeyCursor` is created from a `RecordBatch`, and a set of
/// `PhysicalExpr` that when evaluated on the `RecordBatch` yield the sort keys.
///
/// Additionally it maintains a row cursor that can be advanced through the rows
/// of the provided `RecordBatch`
///
/// `SortKeyCursor::compare` can then be used to compare the sort key pointed to
/// by this row cursor, with that of another `SortKeyCursor`. A cursor stores
/// a row comparator for each other cursor that it is compared to.
pub struct SortKeyCursor {
    stream_idx: usize,
    cur_row: usize,
    num_rows: usize,

    // An id uniquely identifying the record batch scanned by this cursor.
    batch_id: usize,

    rows: Rows,
}

impl std::fmt::Debug for SortKeyCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("SortKeyCursor")
            .field("cur_row", &self.cur_row)
            .field("num_rows", &self.num_rows)
            .field("batch_id", &self.batch_id)
            .finish()
    }
}

impl SortKeyCursor {
    /// Create a new SortKeyCursor
    pub fn new(stream_idx: usize, batch_id: usize, rows: Rows) -> Self {
        Self {
            stream_idx,
            cur_row: 0,
            num_rows: rows.num_rows(),
            batch_id,
            rows,
        }
    }

    #[inline(always)]
    /// Return the stream index of this cursor
    pub fn stream_idx(&self) -> usize {
        self.stream_idx
    }

    #[inline(always)]
    /// Return the batch id of this cursor
    pub fn batch_id(&self) -> usize {
        self.batch_id
    }

    #[inline(always)]
    /// Return true if the stream is finished
    pub fn is_finished(&self) -> bool {
        self.num_rows == self.cur_row
    }

    #[inline(always)]
    /// Returns the cursor's current row, and advances the cursor to the next row
    pub fn advance(&mut self) -> usize {
        assert!(!self.is_finished());
        let t = self.cur_row;
        self.cur_row += 1;
        t
    }

    /// Returns the current row
    fn current(&self) -> Row<'_> {
        self.rows.row(self.cur_row)
    }
}

impl PartialEq for SortKeyCursor {
    fn eq(&self, other: &Self) -> bool {
        self.current() == other.current()
    }
}

impl Eq for SortKeyCursor {}

impl PartialOrd for SortKeyCursor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortKeyCursor {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.is_finished(), other.is_finished()) {
            (true, true) => Ordering::Equal,
            (_, true) => Ordering::Less,
            (true, _) => Ordering::Greater,
            _ => self
                .current()
                .cmp(&other.current())
                .then_with(|| self.stream_idx.cmp(&other.stream_idx)),
        }
    }
}
