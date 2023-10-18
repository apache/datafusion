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

use std::cmp::Ordering;

use arrow::buffer::ScalarBuffer;
use arrow::compute::SortOptions;
use arrow::datatypes::ArrowNativeTypeOp;
use arrow::row::Rows;
use arrow_array::types::ByteArrayType;
use arrow_array::{
    Array, ArrowPrimitiveType, GenericByteArray, OffsetSizeTrait, PrimitiveArray,
};
use arrow_buffer::{Buffer, OffsetBuffer};
use datafusion_execution::memory_pool::MemoryReservation;

/// A comparable collection of values for use with [`Cursor`]
pub trait CursorValues {
    fn len(&self) -> usize;

    fn eq(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> bool;

    fn compare(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> Ordering;
}

/// A comparable cursor, used by sort operations
#[derive(Debug)]
pub struct Cursor<T: CursorValues> {
    offset: usize,
    values: T,
}

impl<T: CursorValues> Cursor<T> {
    /// Create a [`Cursor`] from the given [`CursorValues`]
    pub fn new(values: T) -> Self {
        Self { offset: 0, values }
    }

    /// Returns true if there are no more rows in this cursor
    pub fn is_finished(&self) -> bool {
        self.offset == self.values.len()
    }

    /// Advance the cursor, returning the previous row index
    pub fn advance(&mut self) -> usize {
        let t = self.offset;
        self.offset += 1;
        t
    }
}

impl<T: CursorValues> PartialEq for Cursor<T> {
    fn eq(&self, other: &Self) -> bool {
        T::eq(&self.values, self.offset, &other.values, other.offset)
    }
}

impl<T: CursorValues> Eq for Cursor<T> {}

impl<T: CursorValues> PartialOrd for Cursor<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: CursorValues> Ord for Cursor<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        T::compare(&self.values, self.offset, &other.values, other.offset)
    }
}

/// Implements [`CursorValues`] for [`Rows`]
#[derive(Debug)]
pub struct RowValues {
    rows: Rows,

    /// Tracks for the memory used by in the `Rows` of this
    /// cursor. Freed on drop
    #[allow(dead_code)]
    reservation: MemoryReservation,
}

impl RowValues {
    /// Create a new [`RowValues`] from `rows` and a `reservation`
    /// that tracks its memory. There must be at least one row
    ///
    /// Panics if the reservation is not for exactly `rows.size()`
    /// bytes or if `rows` is empty.
    pub fn new(rows: Rows, reservation: MemoryReservation) -> Self {
        assert_eq!(
            rows.size(),
            reservation.size(),
            "memory reservation mismatch"
        );
        assert!(rows.num_rows() > 0);
        Self { rows, reservation }
    }
}

impl CursorValues for RowValues {
    fn len(&self) -> usize {
        self.rows.num_rows()
    }

    fn eq(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> bool {
        l.rows.row(l_idx) == r.rows.row(r_idx)
    }

    fn compare(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> Ordering {
        l.rows.row(l_idx).cmp(&r.rows.row(r_idx))
    }
}

/// An [`Array`] that can be converted into [`CursorValues`]
pub trait FieldArray: Array + 'static {
    type Values: CursorValues;

    fn values(&self) -> Self::Values;
}

impl<T: ArrowPrimitiveType> FieldArray for PrimitiveArray<T> {
    type Values = PrimitiveValues<T::Native>;

    fn values(&self) -> Self::Values {
        PrimitiveValues(self.values().clone())
    }
}

#[derive(Debug)]
pub struct PrimitiveValues<T: ArrowNativeTypeOp>(ScalarBuffer<T>);

impl<T: ArrowNativeTypeOp> CursorValues for PrimitiveValues<T> {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn eq(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> bool {
        l.0[l_idx].is_eq(r.0[r_idx])
    }

    fn compare(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> Ordering {
        l.0[l_idx].compare(r.0[r_idx])
    }
}

pub struct ByteArrayValues<T: OffsetSizeTrait> {
    offsets: OffsetBuffer<T>,
    values: Buffer,
}

impl<T: OffsetSizeTrait> ByteArrayValues<T> {
    fn value(&self, idx: usize) -> &[u8] {
        let end = self.offsets[idx + 1].as_usize();
        let start = self.offsets[idx].as_usize();
        // Safety: offsets are valid
        unsafe { self.values.get_unchecked(start..end) }
    }
}

impl<T: OffsetSizeTrait> CursorValues for ByteArrayValues<T> {
    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn eq(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> bool {
        l.value(l_idx) == r.value(r_idx)
    }

    fn compare(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> Ordering {
        l.value(l_idx).cmp(r.value(r_idx))
    }
}

impl<T: ByteArrayType> FieldArray for GenericByteArray<T> {
    type Values = ByteArrayValues<T::Offset>;

    fn values(&self) -> Self::Values {
        ByteArrayValues {
            offsets: self.offsets().clone(),
            values: self.values().clone(),
        }
    }
}

/// A collection of sorted, nullable [`FieldArray`]
///
/// Note: comparing cursors with different `SortOptions` will yield an arbitrary ordering
#[derive(Debug)]
pub struct ArrayValues<T: CursorValues> {
    values: T,
    // If nulls first, the first non-null index
    // Otherwise, the first null index
    null_threshold: usize,
    options: SortOptions,
}

impl<T: CursorValues> ArrayValues<T> {
    /// Create a new [`ArrayValues`] from the provided `values` sorted according
    /// to `options`.
    ///
    /// Panics if the array is empty
    pub fn new<A: FieldArray<Values = T>>(options: SortOptions, array: &A) -> Self {
        assert!(array.len() > 0, "Empty array passed to FieldCursor");
        let null_threshold = match options.nulls_first {
            true => array.null_count(),
            false => array.len() - array.null_count(),
        };

        Self {
            values: array.values(),
            null_threshold,
            options,
        }
    }

    fn is_null(&self, idx: usize) -> bool {
        (idx < self.null_threshold) == self.options.nulls_first
    }
}

impl<T: CursorValues> CursorValues for ArrayValues<T> {
    fn len(&self) -> usize {
        self.values.len()
    }

    fn eq(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> bool {
        match (l.is_null(l_idx), r.is_null(r_idx)) {
            (true, true) => true,
            (false, false) => T::eq(&l.values, l_idx, &r.values, r_idx),
            _ => false,
        }
    }

    fn compare(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> Ordering {
        match (l.is_null(l_idx), r.is_null(r_idx)) {
            (true, true) => Ordering::Equal,
            (true, false) => match l.options.nulls_first {
                true => Ordering::Less,
                false => Ordering::Greater,
            },
            (false, true) => match l.options.nulls_first {
                true => Ordering::Greater,
                false => Ordering::Less,
            },
            (false, false) => match l.options.descending {
                true => T::compare(&r.values, r_idx, &l.values, l_idx),
                false => T::compare(&l.values, l_idx, &r.values, r_idx),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_primitive(
        options: SortOptions,
        values: ScalarBuffer<i32>,
        null_count: usize,
    ) -> Cursor<ArrayValues<PrimitiveValues<i32>>> {
        let null_threshold = match options.nulls_first {
            true => null_count,
            false => values.len() - null_count,
        };

        let values = ArrayValues {
            values: PrimitiveValues(values),
            null_threshold,
            options,
        };

        Cursor::new(values)
    }

    #[test]
    fn test_primitive_nulls_first() {
        let options = SortOptions {
            descending: false,
            nulls_first: true,
        };

        let buffer = ScalarBuffer::from(vec![i32::MAX, 1, 2, 3]);
        let mut a = new_primitive(options, buffer, 1);
        let buffer = ScalarBuffer::from(vec![1, 2, -2, -1, 1, 9]);
        let mut b = new_primitive(options, buffer, 2);

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
        let mut a = new_primitive(options, buffer, 2);
        let buffer = ScalarBuffer::from(vec![-1, i32::MAX, i32::MIN]);
        let mut b = new_primitive(options, buffer, 2);

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
        let mut a = new_primitive(options, buffer, 3);
        let buffer = ScalarBuffer::from(vec![67, -3, i32::MAX, i32::MIN]);
        let mut b = new_primitive(options, buffer, 2);

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
        let mut a = new_primitive(options, buffer, 2);
        let buffer = ScalarBuffer::from(vec![i32::MAX, 4546, -3]);
        let mut b = new_primitive(options, buffer, 1);

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
