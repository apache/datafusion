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

use crate::aggregates::group_values::multi_group_by::{
    GroupColumn, Nulls, nulls_equal_to,
};
use crate::aggregates::group_values::null_builder::MaybeNullBufferBuilder;
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanBufferBuilder, FixedSizeBinaryArray,
};
use arrow::buffer::{Buffer, NullBuffer};
use datafusion_common::utils::proxy::VecAllocExt;
use datafusion_common::utils::split_vec_min_alloc;
use datafusion_common::{Result, exec_datafusion_err};
use std::sync::Arc;

/// An implementation of [`GroupColumn`] for `FixedSizeBinary` values
///
/// Stores the group values in a single flat buffer, `byte_width` bytes per
/// value, in a way that allows:
///
/// 1. Efficient comparison of incoming rows to existing rows
/// 2. Efficient construction of the final output array (the buffer is handed
///    to [`FixedSizeBinaryArray`] as-is, no offsets needed)
///
/// Null values occupy `byte_width` zeroed bytes in the buffer so that the
/// value of row `i` is always stored at `i * byte_width..(i + 1) * byte_width`.
pub struct FixedSizeBinaryGroupValueBuilder {
    /// The width in bytes of each value, from `DataType::FixedSizeBinary`
    byte_width: usize,
    /// The flattened group values, `byte_width` bytes per value
    buffer: Vec<u8>,
    /// The number of group values stored
    ///
    /// Tracked explicitly rather than derived from `buffer.len()` because
    /// `byte_width` may be `0`
    len: usize,
    /// Null state (null rows still occupy `byte_width` bytes in `buffer`)
    nulls: MaybeNullBufferBuilder,
}

impl FixedSizeBinaryGroupValueBuilder {
    /// Create a new builder for values of `byte_width` bytes each
    ///
    /// `byte_width` is the width carried by `DataType::FixedSizeBinary` and
    /// must be non-negative (negative widths are rejected by the dispatch in
    /// `make_group_column`)
    pub fn new(byte_width: i32) -> Self {
        debug_assert!(byte_width >= 0);
        Self {
            byte_width: byte_width as usize,
            buffer: Vec::new(),
            len: 0,
            nulls: MaybeNullBufferBuilder::new(),
        }
    }

    fn do_append_val_inner(&mut self, array: &FixedSizeBinaryArray, row: usize) {
        if array.is_null(row) {
            self.nulls.append(true);
            // Null rows still occupy `byte_width` (zeroed) bytes in the
            // buffer so the value offset stays a function of the row index
            self.buffer.resize(self.buffer.len() + self.byte_width, 0);
        } else {
            self.nulls.append(false);
            self.buffer.extend_from_slice(array.value(row));
        }
        self.len += 1;
    }

    fn do_equal_to_inner(
        &self,
        lhs_row: usize,
        array: &FixedSizeBinaryArray,
        rhs_row: usize,
    ) -> bool {
        let exist_null = self.nulls.is_null(lhs_row);
        let input_null = array.is_null(rhs_row);
        if let Some(result) = nulls_equal_to(exist_null, input_null) {
            return result;
        }
        // Otherwise, we need to check their values
        self.value(lhs_row) == array.value(rhs_row)
    }

    /// return the current value of the specified row irrespective of null
    /// (null rows store `byte_width` zeroed bytes)
    pub fn value(&self, row: usize) -> &[u8] {
        let start = row * self.byte_width;
        &self.buffer[start..start + self.byte_width]
    }

    /// Assemble an output array from `values` + `nulls` parts
    ///
    /// Uses `try_new_with_len` rather than `try_new` because the length
    /// cannot be derived from the values buffer when `byte_width == 0`
    fn build_array(
        byte_width: usize,
        values: Vec<u8>,
        nulls: Option<NullBuffer>,
        len: usize,
    ) -> ArrayRef {
        let array = FixedSizeBinaryArray::try_new_with_len(
            byte_width as i32,
            Buffer::from(values),
            nulls,
            len,
        )
        .expect("buffer, nulls and len kept consistent on append");
        Arc::new(array)
    }
}

impl GroupColumn for FixedSizeBinaryGroupValueBuilder {
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool {
        self.do_equal_to_inner(lhs_row, array.as_fixed_size_binary(), rhs_row)
    }

    fn append_val(&mut self, array: &ArrayRef, row: usize) -> Result<()> {
        let arr = array.as_fixed_size_binary();
        debug_assert_eq!(arr.value_size(), self.byte_width);
        self.do_append_val_inner(arr, row);
        Ok(())
    }

    fn vectorized_equal_to(
        &self,
        lhs_rows: &[usize],
        array: &ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut BooleanBufferBuilder,
    ) {
        let array = array.as_fixed_size_binary();

        for (idx, (&lhs_row, &rhs_row)) in
            lhs_rows.iter().zip(rhs_rows.iter()).enumerate()
        {
            // Has found not equal to in previous column, don't need to check
            if !equal_to_results.get_bit(idx) {
                continue;
            }

            if !self.do_equal_to_inner(lhs_row, array, rhs_row) {
                equal_to_results.set_bit(idx, false);
            }
        }
    }

    fn vectorized_append(&mut self, array: &ArrayRef, rows: &[usize]) -> Result<()> {
        let arr = array.as_fixed_size_binary();
        debug_assert_eq!(arr.value_size(), self.byte_width);

        let reserve_bytes = rows.len() * self.byte_width;
        self.buffer.try_reserve(reserve_bytes).map_err(|e| {
            exec_datafusion_err!("failed to reserve {reserve_bytes} bytes: {e}")
        })?;

        let null_count = array.null_count();
        let num_rows = array.len();
        let all_null_or_non_null = if null_count == 0 {
            Nulls::None
        } else if null_count == num_rows {
            Nulls::All
        } else {
            Nulls::Some
        };

        match all_null_or_non_null {
            Nulls::Some => {
                for &row in rows {
                    self.do_append_val_inner(arr, row);
                }
            }

            Nulls::None => {
                self.nulls.append_n(rows.len(), false);
                for &row in rows {
                    self.buffer.extend_from_slice(arr.value(row));
                }
                self.len += rows.len();
            }

            Nulls::All => {
                self.nulls.append_n(rows.len(), true);
                self.buffer
                    .resize(self.buffer.len() + rows.len() * self.byte_width, 0);
                self.len += rows.len();
            }
        }

        Ok(())
    }

    fn len(&self) -> usize {
        self.len
    }

    fn size(&self) -> usize {
        self.buffer.allocated_size() + self.nulls.allocated_size()
    }

    fn build(self: Box<Self>) -> ArrayRef {
        let Self {
            byte_width,
            buffer,
            len,
            nulls,
        } = *self;

        Self::build_array(byte_width, buffer, nulls.build(), len)
    }

    fn take_n(&mut self, n: usize) -> ArrayRef {
        debug_assert!(self.len >= n);

        let null_buffer = self.nulls.take_n(n);
        let first_n = split_vec_min_alloc(&mut self.buffer, n * self.byte_width);
        self.len -= n;

        Self::build_array(self.byte_width, first_n, null_buffer, n)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::aggregates::group_values::multi_group_by::fixed_size_binary::FixedSizeBinaryGroupValueBuilder;
    use arrow::array::{ArrayRef, BooleanBufferBuilder, FixedSizeBinaryArray};

    use super::GroupColumn;

    fn make_true_buffer(n: usize) -> BooleanBufferBuilder {
        let mut buf = BooleanBufferBuilder::new(n);
        buf.append_n(n, true);
        buf
    }

    fn to_vec(buf: &BooleanBufferBuilder) -> Vec<bool> {
        (0..buf.len()).map(|i| buf.get_bit(i)).collect()
    }

    fn make_array(values: Vec<Option<&[u8]>>, byte_width: i32) -> ArrayRef {
        Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                values.into_iter(),
                byte_width,
            )
            .unwrap(),
        )
    }

    #[test]
    fn test_fixed_size_binary_equal_to() {
        let append = |builder: &mut FixedSizeBinaryGroupValueBuilder,
                      builder_array: &ArrayRef,
                      append_rows: &[usize]| {
            for &index in append_rows {
                builder.append_val(builder_array, index).unwrap();
            }
        };

        let equal_to =
            |builder: &FixedSizeBinaryGroupValueBuilder,
             lhs_rows: &[usize],
             input_array: &ArrayRef,
             rhs_rows: &[usize],
             equal_to_results: &mut BooleanBufferBuilder| {
                let iter = lhs_rows.iter().zip(rhs_rows.iter());
                for (idx, (&lhs_row, &rhs_row)) in iter.enumerate() {
                    equal_to_results
                        .set_bit(idx, builder.equal_to(lhs_row, input_array, rhs_row));
                }
            };

        test_fixed_size_binary_equal_to_internal(append, equal_to);
    }

    #[test]
    fn test_fixed_size_binary_vectorized_equal_to() {
        let append = |builder: &mut FixedSizeBinaryGroupValueBuilder,
                      builder_array: &ArrayRef,
                      append_rows: &[usize]| {
            builder
                .vectorized_append(builder_array, append_rows)
                .unwrap();
        };

        let equal_to =
            |builder: &FixedSizeBinaryGroupValueBuilder,
             lhs_rows: &[usize],
             input_array: &ArrayRef,
             rhs_rows: &[usize],
             equal_to_results: &mut BooleanBufferBuilder| {
                builder.vectorized_equal_to(
                    lhs_rows,
                    input_array,
                    rhs_rows,
                    equal_to_results,
                );
            };

        test_fixed_size_binary_equal_to_internal(append, equal_to);
    }

    fn test_fixed_size_binary_equal_to_internal<A, E>(mut append: A, mut equal_to: E)
    where
        A: FnMut(&mut FixedSizeBinaryGroupValueBuilder, &ArrayRef, &[usize]),
        E: FnMut(
            &FixedSizeBinaryGroupValueBuilder,
            &[usize],
            &ArrayRef,
            &[usize],
            &mut BooleanBufferBuilder,
        ),
    {
        // Will cover such cases:
        //   - exist null, input not null
        //   - exist null, input null; values not equal
        //   - exist null, input null; values equal
        //   - exist not null, input null
        //   - exist not null, input not null; values not equal
        //   - exist not null, input not null; values equal

        // Define FixedSizeBinaryGroupValueBuilder
        let mut builder = FixedSizeBinaryGroupValueBuilder::new(3);
        let builder_array = make_array(
            vec![
                None,
                None,
                None,
                Some(b"foo".as_slice()),
                Some(b"bar".as_slice()),
                Some(b"baz".as_slice()),
            ],
            3,
        );
        append(&mut builder, &builder_array, &[0, 1, 2, 3, 4, 5]);

        // Define input array; the value behind the null at row 3 happens to
        // match the existing group value to make sure nulls win over values
        let input_array = make_array(
            vec![
                Some(b"foo".as_slice()),
                None,
                None,
                None,
                Some(b"foo".as_slice()),
                Some(b"baz".as_slice()),
            ],
            3,
        );

        // Check
        let mut equal_to_results = make_true_buffer(builder.len());
        equal_to(
            &builder,
            &[0, 1, 2, 3, 4, 5],
            &input_array,
            &[0, 1, 2, 3, 4, 5],
            &mut equal_to_results,
        );
        let results = to_vec(&equal_to_results);

        assert!(!results[0]);
        assert!(results[1]);
        assert!(results[2]);
        assert!(!results[3]);
        assert!(!results[4]);
        assert!(results[5]);
    }

    #[test]
    fn test_fixed_size_binary_vectorized_operation_special_case() {
        // Test the special `all nulls` or `not nulls` input array case
        // for vectorized append and equal to

        let mut builder = FixedSizeBinaryGroupValueBuilder::new(2);

        // All nulls input array
        let all_nulls_input_array = make_array(vec![None, None, None, None, None], 2);
        builder
            .vectorized_append(&all_nulls_input_array, &[0, 1, 2, 3, 4])
            .unwrap();

        let mut equal_to_results = make_true_buffer(all_nulls_input_array.len());
        builder.vectorized_equal_to(
            &[0, 1, 2, 3, 4],
            &all_nulls_input_array,
            &[0, 1, 2, 3, 4],
            &mut equal_to_results,
        );
        let results = to_vec(&equal_to_results);

        assert!(results[0]);
        assert!(results[1]);
        assert!(results[2]);
        assert!(results[3]);
        assert!(results[4]);

        // All not nulls input array
        let all_not_nulls_input_array = make_array(
            vec![
                Some(b"v1".as_slice()),
                Some(b"v2".as_slice()),
                Some(b"v3".as_slice()),
                Some(b"v4".as_slice()),
                Some(b"v5".as_slice()),
            ],
            2,
        );
        builder
            .vectorized_append(&all_not_nulls_input_array, &[0, 1, 2, 3, 4])
            .unwrap();

        let mut equal_to_results = make_true_buffer(all_not_nulls_input_array.len());
        builder.vectorized_equal_to(
            &[5, 6, 7, 8, 9],
            &all_not_nulls_input_array,
            &[0, 1, 2, 3, 4],
            &mut equal_to_results,
        );
        let results = to_vec(&equal_to_results);

        assert!(results[0]);
        assert!(results[1]);
        assert!(results[2]);
        assert!(results[3]);
        assert!(results[4]);
    }

    #[test]
    fn test_fixed_size_binary_take_n() {
        let mut builder = FixedSizeBinaryGroupValueBuilder::new(2);
        let array = make_array(vec![Some(b"aa".as_slice()), None], 2);
        // aa, null, null
        builder.append_val(&array, 0).unwrap();
        builder.append_val(&array, 1).unwrap();
        builder.append_val(&array, 1).unwrap();

        // (aa, null) remaining: null
        let output = builder.take_n(2);
        assert_eq!(&output, &array);
        assert_eq!(builder.len(), 1);

        // null, aa, null, aa
        builder.append_val(&array, 0).unwrap();
        builder.append_val(&array, 1).unwrap();
        builder.append_val(&array, 0).unwrap();

        // (null, aa) remaining: (null, aa)
        let output = builder.take_n(2);
        let expected = make_array(vec![None, Some(b"aa".as_slice())], 2);
        assert_eq!(&output, &expected);
        assert_eq!(builder.len(), 2);

        // take the remaining (null, aa)
        let output = builder.take_n(2);
        assert_eq!(&output, &expected);
        assert_eq!(builder.len(), 0);
    }

    #[test]
    fn test_fixed_size_binary_build() {
        let mut builder = FixedSizeBinaryGroupValueBuilder::new(2);
        let array = make_array(
            vec![Some(b"aa".as_slice()), None, Some(b"bb".as_slice())],
            2,
        );
        builder.vectorized_append(&array, &[0, 1, 2]).unwrap();
        assert_eq!(builder.len(), 3);

        let output = Box::new(builder).build();
        assert_eq!(&output, &array);
    }

    #[test]
    fn test_zero_width_fixed_size_binary() {
        // A zero byte width is valid per the Arrow spec; the builder must
        // track its length without relying on the (empty) values buffer
        let mut builder = FixedSizeBinaryGroupValueBuilder::new(0);
        let array = make_array(vec![Some(b"".as_slice()), None, Some(b"".as_slice())], 0);

        builder.vectorized_append(&array, &[0, 1, 2]).unwrap();
        assert_eq!(builder.len(), 3);

        // Empty values compare equal, null only equals null
        assert!(builder.equal_to(0, &array, 2));
        assert!(builder.equal_to(1, &array, 1));
        assert!(!builder.equal_to(1, &array, 0));

        let output = builder.take_n(2);
        let expected = make_array(vec![Some(b"".as_slice()), None], 0);
        assert_eq!(&output, &expected);
        assert_eq!(builder.len(), 1);

        let output = Box::new(builder).build();
        let expected = make_array(vec![Some(b"".as_slice())], 0);
        assert_eq!(&output, &expected);
    }
}
