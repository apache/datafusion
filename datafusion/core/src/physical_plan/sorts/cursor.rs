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

/// A [`Cursor`] for [`Rows`]
pub struct RowCursor {
    cur_row: usize,
    num_rows: usize,

    rows: Rows,
}

impl std::fmt::Debug for RowCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("SortKeyCursor")
            .field("cur_row", &self.cur_row)
            .field("num_rows", &self.num_rows)
            .finish()
    }
}

impl RowCursor {
    /// Create a new SortKeyCursor
    pub fn new(rows: Rows) -> Self {
        Self {
            cur_row: 0,
            num_rows: rows.num_rows(),
            rows,
        }
    }

    /// Returns the current row
    fn current(&self) -> Row<'_> {
        self.rows.row(self.cur_row)
    }
}

impl PartialEq for RowCursor {
    fn eq(&self, other: &Self) -> bool {
        self.current() == other.current()
    }
}

impl Eq for RowCursor {}

impl PartialOrd for RowCursor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RowCursor {
    fn cmp(&self, other: &Self) -> Ordering {
        self.current().cmp(&other.current())
    }
}

/// A cursor into a sorted batch of rows
pub trait Cursor: Ord {
    /// Returns true if there are no more rows in this cursor
    fn is_finished(&self) -> bool;

    /// Advance the cursor, returning the previous row index
    fn advance(&mut self) -> usize;
}

impl Cursor for RowCursor {
    #[inline]
    fn is_finished(&self) -> bool {
        self.num_rows == self.cur_row
    }

    #[inline]
    fn advance(&mut self) -> usize {
        let t = self.cur_row;
        self.cur_row += 1;
        t
    }
}
