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
    Array, ArrayRef, AsArray, BooleanBufferBuilder, BufferBuilder, GenericByteArray,
    LargeBinaryArray, LargeStringArray, types::GenericStringType,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{ByteArrayType, DataType, GenericBinaryType};
use datafusion_common::Result;
use datafusion_common::utils::proxy::VecAllocExt;
use datafusion_common::utils::split_vec_min_alloc;
use datafusion_physical_expr_common::binary_map::{INITIAL_BUFFER_CAPACITY, OutputType};
use std::mem::size_of;
use std::sync::Arc;
use std::vec;

/// An implementation of [`GroupColumn`] for binary and utf8 types.
///
/// Stores a collection of binary or utf8 group values in a single buffer
/// in a way that allows:
///
/// 1. Efficient comparison of incoming rows to existing rows
/// 2. Efficient construction of the final output array
///
/// Group values are always accumulated with 64-bit offsets and emitted as
/// `LargeUtf8` / `LargeBinary` arrays, regardless of the offset width of the
/// input arrays, so the concatenation of all distinct group values is not
/// limited to `i32::MAX` bytes. Input arrays may use either offset width;
/// each call dispatches on the array's actual data type.
pub struct ByteGroupValueBuilder {
    output_type: OutputType,
    buffer: BufferBuilder<u8>,
    /// Offsets into `buffer` for each distinct value. These offsets as used
    /// directly to create the final `GenericBinaryArray`. The `i`th string is
    /// stored in the range `offsets[i]..offsets[i+1]` in `buffer`. Null values
    /// are stored as a zero length string.
    offsets: Vec<i64>,
    /// Nulls
    nulls: MaybeNullBufferBuilder,
}

/// Dispatches to `$inner::<B>($($args),*)` with `B` resolved from the
/// builder's output type and the input array's actual offset width.
macro_rules! dispatch_input_type {
    ($self:ident, $column:ident, $inner:ident($($args:expr),*)) => {
        match ($self.output_type, $column.data_type()) {
            (OutputType::Utf8, DataType::Utf8) => {
                $self.$inner::<GenericStringType<i32>>($($args),*)
            }
            (OutputType::Utf8, DataType::LargeUtf8) => {
                $self.$inner::<GenericStringType<i64>>($($args),*)
            }
            (OutputType::Binary, DataType::Binary) => {
                $self.$inner::<GenericBinaryType<i32>>($($args),*)
            }
            (OutputType::Binary, DataType::LargeBinary) => {
                $self.$inner::<GenericBinaryType<i64>>($($args),*)
            }
            (output_type, dt) => unreachable!(
                "ByteGroupValueBuilder: unexpected input type {dt} for output type {output_type:?}"
            ),
        }
    };
}

impl ByteGroupValueBuilder {
    pub fn new(output_type: OutputType) -> Self {
        Self {
            output_type,
            buffer: BufferBuilder::new(INITIAL_BUFFER_CAPACITY),
            offsets: vec![0],
            nulls: MaybeNullBufferBuilder::new(),
        }
    }

    fn equal_to_inner<B>(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool
    where
        B: ByteArrayType,
    {
        let array = array.as_bytes::<B>();
        self.do_equal_to_inner(lhs_row, array, rhs_row)
    }

    fn append_val_inner<B>(&mut self, array: &ArrayRef, row: usize) -> Result<()>
    where
        B: ByteArrayType,
    {
        let arr = array.as_bytes::<B>();
        if arr.is_null(row) {
            self.nulls.append(true);
            // nulls need a zero length in the offset buffer
            let offset = self.buffer.len();
            self.offsets.push(offset as i64);
        } else {
            self.nulls.append(false);
            self.do_append_val_inner(arr, row);
        }

        Ok(())
    }

    fn vectorized_equal_to_inner<B>(
        &self,
        lhs_rows: &[usize],
        array: &ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut BooleanBufferBuilder,
    ) where
        B: ByteArrayType,
    {
        let array = array.as_bytes::<B>();

        for (idx, (&lhs_row, &rhs_row)) in
            lhs_rows.iter().zip(rhs_rows.iter()).enumerate()
        {
            if !equal_to_results.get_bit(idx) {
                continue;
            }

            if !self.do_equal_to_inner(lhs_row, array, rhs_row) {
                equal_to_results.set_bit(idx, false);
            }
        }
    }

    fn vectorized_append_inner<B>(
        &mut self,
        array: &ArrayRef,
        rows: &[usize],
    ) -> Result<()>
    where
        B: ByteArrayType,
    {
        let arr = array.as_bytes::<B>();
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
                    self.append_val_inner::<B>(array, row)?
                }
            }

            Nulls::None => {
                self.nulls.append_n(rows.len(), false);
                for &row in rows {
                    self.do_append_val_inner(arr, row);
                }
            }

            Nulls::All => {
                self.nulls.append_n(rows.len(), true);

                let new_len = self.offsets.len() + rows.len();
                let offset = self.buffer.len();
                self.offsets.resize(new_len, offset as i64);
            }
        }

        Ok(())
    }

    fn do_equal_to_inner<B>(
        &self,
        lhs_row: usize,
        array: &GenericByteArray<B>,
        rhs_row: usize,
    ) -> bool
    where
        B: ByteArrayType,
    {
        let exist_null = self.nulls.is_null(lhs_row);
        let input_null = array.is_null(rhs_row);
        if let Some(result) = nulls_equal_to(exist_null, input_null) {
            return result;
        }
        // Otherwise, we need to check their values
        self.value(lhs_row) == (array.value(rhs_row).as_ref() as &[u8])
    }

    fn do_append_val_inner<B>(&mut self, array: &GenericByteArray<B>, row: usize)
    where
        B: ByteArrayType,
    {
        let value: &[u8] = array.value(row).as_ref();
        self.buffer.append_slice(value);
        self.offsets.push(self.buffer.len() as i64);
    }

    /// return the current value of the specified row irrespective of null
    pub fn value(&self, row: usize) -> &[u8] {
        let l = self.offsets[row] as usize;
        let r = self.offsets[row + 1] as usize;
        // Safety: the offsets are constructed correctly and never decrease
        unsafe { self.buffer.as_slice().get_unchecked(l..r) }
    }
}

impl GroupColumn for ByteGroupValueBuilder {
    fn equal_to(&self, lhs_row: usize, column: &ArrayRef, rhs_row: usize) -> bool {
        dispatch_input_type!(self, column, equal_to_inner(lhs_row, column, rhs_row))
    }

    fn append_val(&mut self, column: &ArrayRef, row: usize) -> Result<()> {
        dispatch_input_type!(self, column, append_val_inner(column, row))
    }

    fn vectorized_equal_to(
        &self,
        lhs_rows: &[usize],
        array: &ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut BooleanBufferBuilder,
    ) {
        dispatch_input_type!(
            self,
            array,
            vectorized_equal_to_inner(lhs_rows, array, rhs_rows, equal_to_results)
        )
    }

    fn vectorized_append(&mut self, column: &ArrayRef, rows: &[usize]) -> Result<()> {
        dispatch_input_type!(self, column, vectorized_append_inner(column, rows))
    }

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn size(&self) -> usize {
        self.buffer.capacity() * size_of::<u8>()
            + self.offsets.allocated_size()
            + self.nulls.allocated_size()
    }

    fn build(self: Box<Self>) -> ArrayRef {
        let Self {
            output_type,
            mut buffer,
            offsets,
            nulls,
        } = *self;

        let null_buffer = nulls.build();

        // SAFETY: the offsets were constructed correctly in `insert_if_new` --
        // monotonically increasing, i64 offsets cannot overflow.
        let offsets = unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(offsets)) };
        let values = buffer.finish();
        match output_type {
            OutputType::Binary => {
                // SAFETY: the offsets were constructed correctly
                Arc::new(unsafe {
                    LargeBinaryArray::new_unchecked(offsets, values, null_buffer)
                })
            }
            OutputType::Utf8 => {
                // SAFETY:
                // 1. the offsets were constructed safely
                //
                // 2. the input arrays were all the correct type and thus since
                // all the values that went in were valid (e.g. utf8) so are all
                // the values that come out
                Arc::new(unsafe {
                    LargeStringArray::new_unchecked(offsets, values, null_buffer)
                })
            }
            _ => unreachable!("View types should use `ArrowBytesViewMap`"),
        }
    }

    fn take_n(&mut self, n: usize) -> ArrayRef {
        debug_assert!(self.len() >= n);
        let null_buffer = self.nulls.take_n(n);
        let first_remaining_offset = self.offsets[n] as usize;

        // Given offsets like [0, 2, 4, 5] and n = 1, we expect to get
        // offsets [0, 2, 3]. We first create two offsets for first_n as [0, 2] and the remaining as [2, 4, 5].
        // And we shift the offset starting from 0 for the remaining one, [2, 4, 5] -> [0, 2, 3].
        let offset_n = self.offsets[n];
        let mut first_n_offsets = split_vec_min_alloc(&mut self.offsets, n);
        // After the split, self.offsets[0] == offset_n in both branches; normalize in-place.
        self.offsets.iter_mut().for_each(|o| *o -= offset_n);
        first_n_offsets.push(offset_n);

        // SAFETY: the offsets were constructed correctly in `insert_if_new` --
        // monotonically increasing, i64 offsets cannot overflow.
        let offsets =
            unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(first_n_offsets)) };

        let mut remaining_buffer =
            BufferBuilder::new(self.buffer.len() - first_remaining_offset);
        // TODO: Current approach copy the remaining and truncate the original one
        // Find out a way to avoid copying buffer but split the original one into two.
        remaining_buffer.append_slice(&self.buffer.as_slice()[first_remaining_offset..]);
        self.buffer.truncate(first_remaining_offset);
        let values = self.buffer.finish();
        self.buffer = remaining_buffer;

        match self.output_type {
            OutputType::Binary => {
                // SAFETY: the offsets were constructed correctly
                Arc::new(unsafe {
                    LargeBinaryArray::new_unchecked(offsets, values, null_buffer)
                })
            }
            OutputType::Utf8 => {
                // SAFETY:
                // 1. the offsets were constructed safely
                //
                // 2. we asserted the input arrays were all the correct type and
                // thus since all the values that went in were valid (e.g. utf8)
                // so are all the values that come out
                Arc::new(unsafe {
                    LargeStringArray::new_unchecked(offsets, values, null_buffer)
                })
            }
            _ => unreachable!("View types should use `ArrowBytesViewMap`"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::aggregates::group_values::multi_group_by::bytes::ByteGroupValueBuilder;
    use arrow::array::{
        ArrayRef, AsArray, BooleanBufferBuilder, LargeStringArray, NullBufferBuilder,
        StringArray,
    };
    use arrow::datatypes::DataType;
    use datafusion_physical_expr::binary_map::OutputType;

    use super::GroupColumn;

    fn make_true_buffer(n: usize) -> BooleanBufferBuilder {
        let mut buf = BooleanBufferBuilder::new(n);
        buf.append_n(n, true);
        buf
    }

    fn to_vec(buf: &BooleanBufferBuilder) -> Vec<bool> {
        (0..buf.len()).map(|i| buf.get_bit(i)).collect()
    }

    #[test]
    fn test_byte_group_value_builder_exceeds_i32_offsets() {
        let mut builder = ByteGroupValueBuilder::new(OutputType::Utf8);

        let large_string = "a".repeat(1024 * 1024);

        let array =
            Arc::new(StringArray::from(vec![Some(large_string.as_str())])) as ArrayRef;

        // Append items until the buffer length exceeds i32::MAX; this used to
        // fail with an "offset overflow" error when the builder used i32
        // offsets for Utf8 input.
        for _ in 0..2049 {
            builder.append_val(&array, 0).unwrap();
        }

        assert_eq!(builder.value(2048), large_string.as_bytes());

        let output = Box::new(builder).build();
        assert_eq!(output.data_type(), &DataType::LargeUtf8);
        assert_eq!(output.len(), 2049);
        assert_eq!(output.as_string::<i64>().value(2048), large_string);
    }

    #[test]
    fn test_byte_take_n() {
        let mut builder = ByteGroupValueBuilder::new(OutputType::Utf8);
        let array = Arc::new(StringArray::from(vec![Some("a"), None])) as ArrayRef;
        // a, null, null
        builder.append_val(&array, 0).unwrap();
        builder.append_val(&array, 1).unwrap();
        builder.append_val(&array, 1).unwrap();

        // (a, null) remaining: null
        let output = builder.take_n(2);
        let expected =
            Arc::new(LargeStringArray::from(vec![Some("a"), None])) as ArrayRef;
        assert_eq!(&output, &expected);

        // null, a, null, a
        builder.append_val(&array, 0).unwrap();
        builder.append_val(&array, 1).unwrap();
        builder.append_val(&array, 0).unwrap();

        // (null, a) remaining: (null, a)
        let output = builder.take_n(2);
        let expected =
            Arc::new(LargeStringArray::from(vec![None, Some("a")])) as ArrayRef;
        assert_eq!(&output, &expected);

        let array = Arc::new(StringArray::from(vec![
            Some("a"),
            None,
            Some("longstringfortest"),
        ])) as ArrayRef;

        // null, a, longstringfortest, null, null
        builder.append_val(&array, 2).unwrap();
        builder.append_val(&array, 1).unwrap();
        builder.append_val(&array, 1).unwrap();

        // (null, a, longstringfortest, null) remaining: (null)
        let output = builder.take_n(4);
        let expected = Arc::new(LargeStringArray::from(vec![
            None,
            Some("a"),
            Some("longstringfortest"),
            None,
        ])) as ArrayRef;
        assert_eq!(&output, &expected);
    }

    #[test]
    fn test_byte_equal_to() {
        let append = |builder: &mut ByteGroupValueBuilder,
                      builder_array: &ArrayRef,
                      append_rows: &[usize]| {
            for &index in append_rows {
                builder.append_val(builder_array, index).unwrap();
            }
        };

        let equal_to =
            |builder: &ByteGroupValueBuilder,
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

        test_byte_equal_to_internal(append, equal_to);
    }

    #[test]
    fn test_byte_vectorized_equal_to() {
        let append = |builder: &mut ByteGroupValueBuilder,
                      builder_array: &ArrayRef,
                      append_rows: &[usize]| {
            builder
                .vectorized_append(builder_array, append_rows)
                .unwrap();
        };

        let equal_to =
            |builder: &ByteGroupValueBuilder,
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

        test_byte_equal_to_internal(append, equal_to);
    }

    #[test]
    fn test_byte_vectorized_operation_special_case() {
        // Test the special `all nulls` or `not nulls` input array case
        // for vectorized append and equal to

        let mut builder = ByteGroupValueBuilder::new(OutputType::Utf8);

        // All nulls input array
        let all_nulls_input_array = Arc::new(StringArray::from(vec![
            Option::<&str>::None,
            None,
            None,
            None,
            None,
        ])) as _;
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
        let all_not_nulls_input_array = Arc::new(StringArray::from(vec![
            Some("string1"),
            Some("string2"),
            Some("string3"),
            Some("string4"),
            Some("string5"),
        ])) as _;
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

    fn test_byte_equal_to_internal<A, E>(mut append: A, mut equal_to: E)
    where
        A: FnMut(&mut ByteGroupValueBuilder, &ArrayRef, &[usize]),
        E: FnMut(
            &ByteGroupValueBuilder,
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

        // Define ByteGroupValueBuilder
        let mut builder = ByteGroupValueBuilder::new(OutputType::Utf8);
        let builder_array = Arc::new(StringArray::from(vec![
            None,
            None,
            None,
            Some("foo"),
            Some("bar"),
            Some("baz"),
        ])) as ArrayRef;
        append(&mut builder, &builder_array, &[0, 1, 2, 3, 4, 5]);

        // Define input array
        let (offsets, buffer, _nulls) = StringArray::from(vec![
            Some("foo"),
            Some("bar"),
            None,
            None,
            Some("foo"),
            Some("baz"),
        ])
        .into_parts();

        // explicitly build a null buffer where one of the null values also happens to match
        let mut nulls = NullBufferBuilder::new(6);
        nulls.append_non_null();
        nulls.append_null();
        nulls.append_null();
        nulls.append_null();
        nulls.append_non_null();
        nulls.append_non_null();
        let input_array =
            Arc::new(StringArray::new(offsets, buffer, nulls.finish())) as ArrayRef;

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
}
