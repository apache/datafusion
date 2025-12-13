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
    Array, ArrayRef, AsArray, BufferBuilder, GenericBinaryArray, GenericByteArray,
    GenericStringArray, OffsetSizeTrait, types::GenericStringType,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{ByteArrayType, DataType, GenericBinaryType};
use datafusion_common::utils::proxy::VecAllocExt;
use datafusion_common::{Result, exec_datafusion_err};
use datafusion_physical_expr_common::binary_map::{INITIAL_BUFFER_CAPACITY, OutputType};
use itertools::izip;
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
pub struct ByteGroupValueBuilder<O>
where
    O: OffsetSizeTrait,
{
    output_type: OutputType,
    buffer: BufferBuilder<u8>,
    /// Offsets into `buffer` for each distinct value. These offsets as used
    /// directly to create the final `GenericBinaryArray`. The `i`th string is
    /// stored in the range `offsets[i]..offsets[i+1]` in `buffer`. Null values
    /// are stored as a zero length string.
    offsets: Vec<O>,
    /// Nulls
    nulls: MaybeNullBufferBuilder,
    /// The maximum size of the buffer for `0`
    max_buffer_size: usize,
}

impl<O> ByteGroupValueBuilder<O>
where
    O: OffsetSizeTrait,
{
    pub fn new(output_type: OutputType) -> Self {
        Self {
            output_type,
            buffer: BufferBuilder::new(INITIAL_BUFFER_CAPACITY),
            offsets: vec![O::default()],
            nulls: MaybeNullBufferBuilder::new(),
            max_buffer_size: if O::IS_LARGE {
                i64::MAX as usize
            } else {
                i32::MAX as usize
            },
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
            self.offsets.push(O::usize_as(offset));
        } else {
            self.nulls.append(false);
            self.do_append_val_inner(arr, row)?;
        }

        Ok(())
    }

    fn vectorized_equal_to_inner<B>(
        &self,
        lhs_rows: &[usize],
        array: &ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut [bool],
    ) where
        B: ByteArrayType,
    {
        let array = array.as_bytes::<B>();

        let iter = izip!(
            lhs_rows.iter(),
            rhs_rows.iter(),
            equal_to_results.iter_mut(),
        );

        for (&lhs_row, &rhs_row, equal_to_result) in iter {
            // Has found not equal to, don't need to check
            if !*equal_to_result {
                continue;
            }

            *equal_to_result = self.do_equal_to_inner(lhs_row, array, rhs_row);
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
                    self.do_append_val_inner(arr, row)?;
                }
            }

            Nulls::All => {
                self.nulls.append_n(rows.len(), true);

                let new_len = self.offsets.len() + rows.len();
                let offset = self.buffer.len();
                self.offsets.resize(new_len, O::usize_as(offset));
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

    fn do_append_val_inner<B>(
        &mut self,
        array: &GenericByteArray<B>,
        row: usize,
    ) -> Result<()>
    where
        B: ByteArrayType,
    {
        let value: &[u8] = array.value(row).as_ref();
        self.buffer.append_slice(value);

        if self.buffer.len() > self.max_buffer_size {
            return Err(exec_datafusion_err!(
                "offset overflow, buffer size > {}",
                self.max_buffer_size
            ));
        }

        self.offsets.push(O::usize_as(self.buffer.len()));
        Ok(())
    }

    /// return the current value of the specified row irrespective of null
    pub fn value(&self, row: usize) -> &[u8] {
        let l = self.offsets[row].as_usize();
        let r = self.offsets[row + 1].as_usize();
        // Safety: the offsets are constructed correctly and never decrease
        unsafe { self.buffer.as_slice().get_unchecked(l..r) }
    }
}

impl<O> GroupColumn for ByteGroupValueBuilder<O>
where
    O: OffsetSizeTrait,
{
    fn equal_to(&self, lhs_row: usize, column: &ArrayRef, rhs_row: usize) -> bool {
        // Sanity array type
        match self.output_type {
            OutputType::Binary => {
                debug_assert!(matches!(
                    column.data_type(),
                    DataType::Binary | DataType::LargeBinary
                ));
                self.equal_to_inner::<GenericBinaryType<O>>(lhs_row, column, rhs_row)
            }
            OutputType::Utf8 => {
                debug_assert!(matches!(
                    column.data_type(),
                    DataType::Utf8 | DataType::LargeUtf8
                ));
                self.equal_to_inner::<GenericStringType<O>>(lhs_row, column, rhs_row)
            }
            _ => unreachable!("View types should use `ArrowBytesViewMap`"),
        }
    }

    fn append_val(&mut self, column: &ArrayRef, row: usize) -> Result<()> {
        // Sanity array type
        match self.output_type {
            OutputType::Binary => {
                debug_assert!(matches!(
                    column.data_type(),
                    DataType::Binary | DataType::LargeBinary
                ));
                self.append_val_inner::<GenericBinaryType<O>>(column, row)?
            }
            OutputType::Utf8 => {
                debug_assert!(matches!(
                    column.data_type(),
                    DataType::Utf8 | DataType::LargeUtf8
                ));
                self.append_val_inner::<GenericStringType<O>>(column, row)?
            }
            _ => unreachable!("View types should use `ArrowBytesViewMap`"),
        };

        Ok(())
    }

    fn vectorized_equal_to(
        &self,
        lhs_rows: &[usize],
        array: &ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut [bool],
    ) {
        // Sanity array type
        match self.output_type {
            OutputType::Binary => {
                debug_assert!(matches!(
                    array.data_type(),
                    DataType::Binary | DataType::LargeBinary
                ));
                self.vectorized_equal_to_inner::<GenericBinaryType<O>>(
                    lhs_rows,
                    array,
                    rhs_rows,
                    equal_to_results,
                );
            }
            OutputType::Utf8 => {
                debug_assert!(matches!(
                    array.data_type(),
                    DataType::Utf8 | DataType::LargeUtf8
                ));
                self.vectorized_equal_to_inner::<GenericStringType<O>>(
                    lhs_rows,
                    array,
                    rhs_rows,
                    equal_to_results,
                );
            }
            _ => unreachable!("View types should use `ArrowBytesViewMap`"),
        }
    }

    fn vectorized_append(&mut self, column: &ArrayRef, rows: &[usize]) -> Result<()> {
        match self.output_type {
            OutputType::Binary => {
                debug_assert!(matches!(
                    column.data_type(),
                    DataType::Binary | DataType::LargeBinary
                ));
                self.vectorized_append_inner::<GenericBinaryType<O>>(column, rows)?
            }
            OutputType::Utf8 => {
                debug_assert!(matches!(
                    column.data_type(),
                    DataType::Utf8 | DataType::LargeUtf8
                ));
                self.vectorized_append_inner::<GenericStringType<O>>(column, rows)?
            }
            _ => unreachable!("View types should use `ArrowBytesViewMap`"),
        };

        Ok(())
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
            ..
        } = *self;

        let null_buffer = nulls.build();

        // SAFETY: the offsets were constructed correctly in `insert_if_new` --
        // monotonically increasing, overflows were checked.
        let offsets = unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(offsets)) };
        let values = buffer.finish();
        match output_type {
            OutputType::Binary => {
                // SAFETY: the offsets were constructed correctly
                Arc::new(unsafe {
                    GenericBinaryArray::new_unchecked(offsets, values, null_buffer)
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
                    GenericStringArray::new_unchecked(offsets, values, null_buffer)
                })
            }
            _ => unreachable!("View types should use `ArrowBytesViewMap`"),
        }
    }

    fn take_n(&mut self, n: usize) -> ArrayRef {
        debug_assert!(self.len() >= n);
        let null_buffer = self.nulls.take_n(n);
        let first_remaining_offset = O::as_usize(self.offsets[n]);

        // Given offsets like [0, 2, 4, 5] and n = 1, we expect to get
        // offsets [0, 2, 3]. We first create two offsets for first_n as [0, 2] and the remaining as [2, 4, 5].
        // And we shift the offset starting from 0 for the remaining one, [2, 4, 5] -> [0, 2, 3].
        let mut first_n_offsets = self.offsets.drain(0..n).collect::<Vec<_>>();
        let offset_n = *self.offsets.first().unwrap();
        self.offsets
            .iter_mut()
            .for_each(|offset| *offset = offset.sub(offset_n));
        first_n_offsets.push(offset_n);

        // SAFETY: the offsets were constructed correctly in `insert_if_new` --
        // monotonically increasing, overflows were checked.
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
                    GenericBinaryArray::new_unchecked(offsets, values, null_buffer)
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
                    GenericStringArray::new_unchecked(offsets, values, null_buffer)
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
    use arrow::array::{ArrayRef, NullBufferBuilder, StringArray};
    use datafusion_common::DataFusionError;
    use datafusion_physical_expr::binary_map::OutputType;

    use super::GroupColumn;

    #[test]
    fn test_byte_group_value_builder_overflow() {
        let mut builder = ByteGroupValueBuilder::<i32>::new(OutputType::Utf8);

        let large_string = "a".repeat(1024 * 1024);

        let array =
            Arc::new(StringArray::from(vec![Some(large_string.as_str())])) as ArrayRef;

        // Append items until our buffer length is i32::MAX as usize
        for _ in 0..2047 {
            builder.append_val(&array, 0).unwrap();
        }

        assert!(matches!(
            builder.append_val(&array, 0),
            Err(DataFusionError::Execution(e)) if e.contains("offset overflow")
        ));

        assert_eq!(builder.value(2046), large_string.as_bytes());
    }

    #[test]
    fn test_byte_take_n() {
        let mut builder = ByteGroupValueBuilder::<i32>::new(OutputType::Utf8);
        let array = Arc::new(StringArray::from(vec![Some("a"), None])) as ArrayRef;
        // a, null, null
        builder.append_val(&array, 0).unwrap();
        builder.append_val(&array, 1).unwrap();
        builder.append_val(&array, 1).unwrap();

        // (a, null) remaining: null
        let output = builder.take_n(2);
        assert_eq!(&output, &array);

        // null, a, null, a
        builder.append_val(&array, 0).unwrap();
        builder.append_val(&array, 1).unwrap();
        builder.append_val(&array, 0).unwrap();

        // (null, a) remaining: (null, a)
        let output = builder.take_n(2);
        let array = Arc::new(StringArray::from(vec![None, Some("a")])) as ArrayRef;
        assert_eq!(&output, &array);

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
        let array = Arc::new(StringArray::from(vec![
            None,
            Some("a"),
            Some("longstringfortest"),
            None,
        ])) as ArrayRef;
        assert_eq!(&output, &array);
    }

    #[test]
    fn test_byte_equal_to() {
        let append = |builder: &mut ByteGroupValueBuilder<i32>,
                      builder_array: &ArrayRef,
                      append_rows: &[usize]| {
            for &index in append_rows {
                builder.append_val(builder_array, index).unwrap();
            }
        };

        let equal_to = |builder: &ByteGroupValueBuilder<i32>,
                        lhs_rows: &[usize],
                        input_array: &ArrayRef,
                        rhs_rows: &[usize],
                        equal_to_results: &mut Vec<bool>| {
            let iter = lhs_rows.iter().zip(rhs_rows.iter());
            for (idx, (&lhs_row, &rhs_row)) in iter.enumerate() {
                equal_to_results[idx] = builder.equal_to(lhs_row, input_array, rhs_row);
            }
        };

        test_byte_equal_to_internal(append, equal_to);
    }

    #[test]
    fn test_byte_vectorized_equal_to() {
        let append = |builder: &mut ByteGroupValueBuilder<i32>,
                      builder_array: &ArrayRef,
                      append_rows: &[usize]| {
            builder
                .vectorized_append(builder_array, append_rows)
                .unwrap();
        };

        let equal_to = |builder: &ByteGroupValueBuilder<i32>,
                        lhs_rows: &[usize],
                        input_array: &ArrayRef,
                        rhs_rows: &[usize],
                        equal_to_results: &mut Vec<bool>| {
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

        let mut builder = ByteGroupValueBuilder::<i32>::new(OutputType::Utf8);

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

        let mut equal_to_results = vec![true; all_nulls_input_array.len()];
        builder.vectorized_equal_to(
            &[0, 1, 2, 3, 4],
            &all_nulls_input_array,
            &[0, 1, 2, 3, 4],
            &mut equal_to_results,
        );

        assert!(equal_to_results[0]);
        assert!(equal_to_results[1]);
        assert!(equal_to_results[2]);
        assert!(equal_to_results[3]);
        assert!(equal_to_results[4]);

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

        let mut equal_to_results = vec![true; all_not_nulls_input_array.len()];
        builder.vectorized_equal_to(
            &[5, 6, 7, 8, 9],
            &all_not_nulls_input_array,
            &[0, 1, 2, 3, 4],
            &mut equal_to_results,
        );

        assert!(equal_to_results[0]);
        assert!(equal_to_results[1]);
        assert!(equal_to_results[2]);
        assert!(equal_to_results[3]);
        assert!(equal_to_results[4]);
    }

    fn test_byte_equal_to_internal<A, E>(mut append: A, mut equal_to: E)
    where
        A: FnMut(&mut ByteGroupValueBuilder<i32>, &ArrayRef, &[usize]),
        E: FnMut(
            &ByteGroupValueBuilder<i32>,
            &[usize],
            &ArrayRef,
            &[usize],
            &mut Vec<bool>,
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
        let mut builder = ByteGroupValueBuilder::<i32>::new(OutputType::Utf8);
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

        // explicitly build a boolean buffer where one of the null values also happens to match
        let mut nulls = NullBufferBuilder::new(6);
        nulls.append_non_null();
        nulls.append_null(); // this sets Some("bar") to null above
        nulls.append_null();
        nulls.append_null();
        nulls.append_non_null();
        nulls.append_non_null();
        let input_array =
            Arc::new(StringArray::new(offsets, buffer, nulls.finish())) as ArrayRef;

        // Check
        let mut equal_to_results = vec![true; builder.len()];
        equal_to(
            &builder,
            &[0, 1, 2, 3, 4, 5],
            &input_array,
            &[0, 1, 2, 3, 4, 5],
            &mut equal_to_results,
        );

        assert!(!equal_to_results[0]);
        assert!(equal_to_results[1]);
        assert!(equal_to_results[2]);
        assert!(!equal_to_results[3]);
        assert!(!equal_to_results[4]);
        assert!(equal_to_results[5]);
    }
}
