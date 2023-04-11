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

use crate::physical_plan::sorts::sort::SortOptions;
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::ArrowNativeTypeOp;
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

/// A cursor over sorted, nullable [`ArrowNativeTypeOp`]
///
/// Note: comparing cursors with different `SortOptions` will yield an arbitrary ordering
#[derive(Debug)]
pub struct PrimitiveCursor<T: ArrowNativeTypeOp> {
    values: ScalarBuffer<T>,
    offset: usize,
    // If nulls first, the first non-null index
    // Otherwise, the first null index
    null_threshold: usize,
    options: SortOptions,
}

impl<T: ArrowNativeTypeOp> PrimitiveCursor<T> {
    /// Create a new [`PrimitiveCursor`] from the provided `values` sorted according to `options`
    pub fn new(options: SortOptions, values: ScalarBuffer<T>, null_count: usize) -> Self {
        assert!(null_count <= values.len());

        let null_threshold = match options.nulls_first {
            true => null_count,
            false => values.len() - null_count,
        };

        Self {
            values,
            offset: 0,
            null_threshold,
            options,
        }
    }

    fn is_null(&self) -> bool {
        (self.offset < self.null_threshold) == self.options.nulls_first
    }

    fn value(&self) -> T {
        self.values[self.offset]
    }
}

impl<T: ArrowNativeTypeOp> PartialEq for PrimitiveCursor<T> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl<T: ArrowNativeTypeOp> Eq for PrimitiveCursor<T> {}
impl<T: ArrowNativeTypeOp> PartialOrd for PrimitiveCursor<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: ArrowNativeTypeOp> Ord for PrimitiveCursor<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.is_null(), other.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => match self.options.nulls_first {
                true => Ordering::Less,
                false => Ordering::Greater,
            },
            (false, true) => match self.options.nulls_first {
                true => Ordering::Greater,
                false => Ordering::Less,
            },
            (false, false) => {
                let s_v = self.value();
                let o_v = other.value();

                match self.options.descending {
                    true => o_v.compare(s_v),
                    false => s_v.compare(o_v),
                }
            }
        }
    }
}

impl<T: ArrowNativeTypeOp> Cursor for PrimitiveCursor<T> {
    fn is_finished(&self) -> bool {
        self.offset == self.values.len()
    }

    fn advance(&mut self) -> usize {
        let t = self.offset;
        self.offset += 1;
        t
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_primitive_nulls_first() {
        let options = SortOptions {
            descending: false,
            nulls_first: true,
        };

        let buffer = ScalarBuffer::from(vec![i32::MAX, 1, 2, 3]);
        let mut a = PrimitiveCursor::new(options, buffer, 1);
        let buffer = ScalarBuffer::from(vec![1, 2, -2, -1, 1, 9]);
        let mut b = PrimitiveCursor::new(options, buffer, 2);

        // NULL == NULL
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);

        // NULL == NULL
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);

        // NULL < -2
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // 1 > -2
        a.advance();
        assert_eq!(a.cmp(&b), Ordering::Greater);

        // 1 > -1
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Greater);

        // 1 == 1
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);

        // 9 > 1
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // 9 > 2
        a.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        let options = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let buffer = ScalarBuffer::from(vec![0, 1, i32::MIN, i32::MAX]);
        let mut a = PrimitiveCursor::new(options, buffer, 2);
        let buffer = ScalarBuffer::from(vec![-1, i32::MAX, i32::MIN]);
        let mut b = PrimitiveCursor::new(options, buffer, 2);

        // 0 > -1
        assert_eq!(a.cmp(&b), Ordering::Greater);

        // 0 < NULL
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // 1 < NULL
        a.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // NULL = NULL
        a.advance();
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);

        let options = SortOptions {
            descending: true,
            nulls_first: false,
        };

        let buffer = ScalarBuffer::from(vec![6, 1, i32::MIN, i32::MAX]);
        let mut a = PrimitiveCursor::new(options, buffer, 3);
        let buffer = ScalarBuffer::from(vec![67, -3, i32::MAX, i32::MIN]);
        let mut b = PrimitiveCursor::new(options, buffer, 2);

        // 6 > 67
        assert_eq!(a.cmp(&b), Ordering::Greater);

        // 6 < -3
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // 6 < NULL
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // 6 < NULL
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // NULL == NULL
        a.advance();
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);

        let options = SortOptions {
            descending: true,
            nulls_first: true,
        };

        let buffer = ScalarBuffer::from(vec![i32::MIN, i32::MAX, 6, 3]);
        let mut a = PrimitiveCursor::new(options, buffer, 2);
        let buffer = ScalarBuffer::from(vec![i32::MAX, 4546, -3]);
        let mut b = PrimitiveCursor::new(options, buffer, 1);

        // NULL == NULL
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);

        // NULL == NULL
        a.advance();
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);

        // NULL < 4546
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // 6 > 4546
        a.advance();
        assert_eq!(a.cmp(&b), Ordering::Greater);

        // 6 < -3
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);
    }
}
