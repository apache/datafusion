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

use crate::aggregates_blocked::group_values::multi_group_by::{
    BlockedGroupColumn, Nulls, nulls_equal_to,
};
use crate::aggregates_blocked::group_values::null_builder::NullBufferBuilderExt;
use arrow::array::{Array, ArrayRef, AsArray, BooleanBufferBuilder, BufferBuilder, GenericBinaryArray, GenericByteArray, GenericStringArray, NullBufferBuilder, OffsetSizeTrait, types::GenericStringType};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{ByteArrayType, DataType, GenericBinaryType};
use datafusion_common::utils::proxy::VecAllocExt;
use datafusion_common::{Result, exec_datafusion_err};
use datafusion_expr_common::blocked_helpers::BlockedByteArrayBuilder;
use datafusion_expr_common::groups_accumulator::{BlockedGroupSelection, BlocksIndex};
use datafusion_physical_expr_common::binary_map::OutputType;
use std::sync::Arc;

fn checked_output_offset<O: OffsetSizeTrait>(
    current_len: usize,
    additional_len: usize,
) -> Result<O> {
    current_len
      .checked_add(additional_len)
      .and_then(O::from_usize)
        .ok_or_else(|| exec_datafusion_err!("Offset overflow while copying group values"))
}

/// An implementation of [`GroupColumn`] for binary and utf8 types.
///
/// Stores a collection of binary or utf8 group values in a single buffer
/// in a way that allows:
///
/// 1. Efficient comparison of incoming rows to existing rows
/// 2. Efficient construction of the final output array
pub struct ByteGroupValueBuilder<const IS_FIXED_BLOCK: bool, O>
where
    O: OffsetSizeTrait,
{
    output_type: OutputType,
    data: BlockedByteArrayBuilder<IS_FIXED_BLOCK, GenericBinaryType<O>>,
    // buffer: BufferBuilder<u8>,
    // /// Offsets into `buffer` for each distinct value. These offsets as used
    // /// directly to create the final `GenericBinaryArray`. The `i`th string is
    // /// stored in the range `offsets[i]..offsets[i+1]` in `buffer`. Null values
    // /// are stored as a zero length string.
    // offsets: Vec<O>,
    // /// Nulls
    // nulls: NullBufferBuilder,
    /// The maximum size of the buffer for `0`
    max_buffer_size: usize,
}

impl<const IS_FIXED_BLOCK: bool, O> ByteGroupValueBuilder<IS_FIXED_BLOCK, O>
where
    O: OffsetSizeTrait,
{
    pub fn new(output_type: OutputType, block_size: usize) -> Self {
        Self {
            output_type,
            data: BlockedByteArrayBuilder::new(block_size),
            // buffer: BufferBuilder::new(INITIAL_BUFFER_CAPACITY),
            // offsets: vec![O::default()],
            // nulls: NullBufferBuilder::empty(),
            max_buffer_size: if O::IS_LARGE {
                i64::MAX as usize
            } else {
                i32::MAX as usize
            },
        }
    }

    fn equal_to_inner<B>(&self, lhs_row: BlocksIndex, array: &ArrayRef, rhs_row: usize) -> bool
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
            self.data.push_null();
        } else {
            self.data.append_valid();
            self.do_append_val_inner(arr, row)?;
        }

        Ok(())
    }

    fn vectorized_equal_to_inner<B>(
        &self,
        lhs_rows: &[BlocksIndex],
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
                self.data.append_n_valids(rows.len());
                for &row in rows {
                    self.do_append_val_inner(arr, row)?;
                }
            }

            Nulls::All => {
                self.data.append_n_nulls(rows.len());
            }
        }

        Ok(())
    }

    fn do_equal_to_inner<B>(
        &self,
        lhs_row: BlocksIndex,
        array: &GenericByteArray<B>,
        rhs_row: usize,
    ) -> bool
    where
        B: ByteArrayType,
    {
        let exist_null = self.data.is_null(lhs_row);
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

        if self.data.current_block_bytes_len() + value.len() > self.max_buffer_size {
            return Err(exec_datafusion_err!(
                "offset overflow, buffer size > {}",
                self.max_buffer_size
            ));
        }

        self.data.append_valid_slice(value);

        Ok(())
    }

    /// return the current value of the specified row irrespective of null
    pub fn value(&self, row: BlocksIndex) -> &[u8] {
        self.data.value_bytes(row)
    }
}

impl<const IS_FIXED_BLOCK: bool, O> BlockedGroupColumn<IS_FIXED_BLOCK> for ByteGroupValueBuilder<IS_FIXED_BLOCK, O>
where
    O: OffsetSizeTrait,
{
    fn batch_size(&self) -> usize {
        self.data.block_size()
    }
    fn equal_to(&self, lhs_row: BlocksIndex, column: &ArrayRef, rhs_row: usize) -> bool {
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
        }

        Ok(())
    }

    fn vectorized_equal_to(
        &self,
        lhs_rows: &[BlocksIndex],
        array: &ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut BooleanBufferBuilder,
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
        }

        Ok(())
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn size(&self) -> usize {
        self.data.allocated_size()
    }

    fn values_preserving(&self, selection: BlockedGroupSelection<'_>) -> Result<ArrayRef> {
        selection.validate_num_groups(self.len())?;
        let mut buffer = BufferBuilder::<u8>::new(0);
        let mut offsets = Vec::with_capacity(selection.len() + 1);
        let mut nulls = NullBufferBuilder::new(selection.len());
        offsets.push(O::default());

        for index in selection.iter() {
            let is_null = self.data.is_null(index);
            let value = if is_null { &[] } else { self.value(index) };
            let offset = checked_output_offset::<O>(buffer.len(), value.len())?;

            nulls.append(!is_null);
            buffer.append_slice(value);
            offsets.push(offset);
        }

        // SAFETY: every offset was checked for representability and is the
        // monotonically increasing length of `buffer` after an append.
        let offsets = unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(offsets)) };
        let values = buffer.finish();
        let nulls = nulls.build();
        Ok(match self.output_type {
            OutputType::Binary => Arc::new(unsafe {
                GenericBinaryArray::new_unchecked(offsets, values, nulls)
            }),
            OutputType::Utf8 => Arc::new(unsafe {
                GenericStringArray::new_unchecked(offsets, values, nulls)
            }),
            _ => unreachable!("View types should use `ArrowBytesViewMap`"),
        })
    }

    fn take_all(self: Box<Self>) -> Vec<ArrayRef> {
        let Self { mut data, output_type, max_buffer_size: _ } = *self;
        // SAFETY: offsets are constructed valid
        let arrays = unsafe {data.take_all_unchecked()};

            arrays.into_iter().map(|array| Self::build_array(output_type, array)).collect()
    }

    fn take_next_block(&mut self) -> Option<ArrayRef> {
        // SAFETY: the offsets were constructed correctly

        let data = unsafe { self.data.take_block_unchecked() }?;

        Some(Self::build_array(self.output_type, data))
    }

    fn take_n(&mut self, n: usize) -> ArrayRef {
        debug_assert!(self.len() >= n);
        // SAFETY: the offsets were constructed correctly

        let data = unsafe { self.data.take_n_unchecked(n, None::<std::iter::Empty<_>>) };

        Self::build_array(self.output_type, data)
    }

    fn start_new_block(&mut self) {
        self.data.start_new_block();
    }
}

impl<O, const IS_FIXED_BLOCK: bool> ByteGroupValueBuilder<IS_FIXED_BLOCK, O>
where
  O: OffsetSizeTrait,
{
    fn build_array(output_type: OutputType, array: GenericByteArray<GenericBinaryType<O>>) -> ArrayRef {
        match output_type {
            OutputType::Binary => {
                Arc::new(array) as ArrayRef
            }
            OutputType::Utf8 => {
                // SAFETY:
                // 1. the offsets were constructed safely
                //
                // 2. we asserted the input arrays were all the correct type and
                // thus since all the values that went in were valid (e.g. utf8)
                // so are all the values that come out
                let (offsets, values, null_buffer) = array.into_parts();

                Arc::new(unsafe {
                    GenericStringArray::new_unchecked(offsets, values, null_buffer)
                }) as ArrayRef
            }
            _ => unreachable!("View types should use `ArrowBytesViewMap`"),
        }
    }
}
//
// #[cfg(test)]
// mod tests {
//     use std::sync::Arc;
//
//     use crate::aggregates::group_values::multi_group_by::bytes::ByteGroupValueBuilder;
//     use arrow::array::{ArrayRef, BooleanBufferBuilder, NullBufferBuilder, StringArray};
//     use datafusion_common::DataFusionError;
//     use datafusion_physical_expr::binary_map::OutputType;
//
//     use super::{GroupColumn, checked_output_offset};
//
//     fn make_true_buffer(n: usize) -> BooleanBufferBuilder {
//         let mut buf = BooleanBufferBuilder::new(n);
//         buf.append_n(n, true);
//         buf
//     }
//
//     fn to_vec(buf: &BooleanBufferBuilder) -> Vec<bool> {
//         (0..buf.len()).map(|i| buf.get_bit(i)).collect()
//     }
//
//     #[test]
//     fn test_selected_copy_offset_overflow_is_checked() {
//         assert_eq!(
//             checked_output_offset::<i32>(i32::MAX as usize, 0).unwrap(),
//             i32::MAX
//         );
//         assert!(matches!(
//             checked_output_offset::<i32>(i32::MAX as usize, 1),
//             Err(DataFusionError::Execution(e)) if e.contains("Offset overflow")
//         ));
//         assert!(checked_output_offset::<i64>(usize::MAX, 1).is_err());
//     }
//
//     #[test]
//     fn test_byte_group_value_builder_overflow() {
//         let mut builder = ByteGroupValueBuilder::<i32>::new(OutputType::Utf8);
//
//         let large_string = "a".repeat(1024 * 1024);
//
//         let array =
//             Arc::new(StringArray::from(vec![Some(large_string.as_str())])) as ArrayRef;
//
//         // Append items until our buffer length is i32::MAX as usize
//         for _ in 0..2047 {
//             builder.append_val(&array, 0).unwrap();
//         }
//
//         assert!(matches!(
//             builder.append_val(&array, 0),
//             Err(DataFusionError::Execution(e)) if e.contains("offset overflow")
//         ));
//
//         assert_eq!(builder.value(2046), large_string.as_bytes());
//     }
//
//     #[test]
//     fn test_byte_take_n() {
//         let mut builder = ByteGroupValueBuilder::<i32>::new(OutputType::Utf8);
//         let array = Arc::new(StringArray::from(vec![Some("a"), None])) as ArrayRef;
//         // a, null, null
//         builder.append_val(&array, 0).unwrap();
//         builder.append_val(&array, 1).unwrap();
//         builder.append_val(&array, 1).unwrap();
//
//         // (a, null) remaining: null
//         let output = builder.take_n(2);
//         assert_eq!(&output, &array);
//
//         // null, a, null, a
//         builder.append_val(&array, 0).unwrap();
//         builder.append_val(&array, 1).unwrap();
//         builder.append_val(&array, 0).unwrap();
//
//         // (null, a) remaining: (null, a)
//         let output = builder.take_n(2);
//         let array = Arc::new(StringArray::from(vec![None, Some("a")])) as ArrayRef;
//         assert_eq!(&output, &array);
//
//         let array = Arc::new(StringArray::from(vec![
//             Some("a"),
//             None,
//             Some("longstringfortest"),
//         ])) as ArrayRef;
//
//         // null, a, longstringfortest, null, null
//         builder.append_val(&array, 2).unwrap();
//         builder.append_val(&array, 1).unwrap();
//         builder.append_val(&array, 1).unwrap();
//
//         // (null, a, longstringfortest, null) remaining: (null)
//         let output = builder.take_n(4);
//         let array = Arc::new(StringArray::from(vec![
//             None,
//             Some("a"),
//             Some("longstringfortest"),
//             None,
//         ])) as ArrayRef;
//         assert_eq!(&output, &array);
//     }
//
//     #[test]
//     fn test_byte_equal_to() {
//         let append = |builder: &mut ByteGroupValueBuilder<i32>,
//                       builder_array: &ArrayRef,
//                       append_rows: &[usize]| {
//             for &index in append_rows {
//                 builder.append_val(builder_array, index).unwrap();
//             }
//         };
//
//         let equal_to =
//             |builder: &ByteGroupValueBuilder<i32>,
//              lhs_rows: &[usize],
//              input_array: &ArrayRef,
//              rhs_rows: &[usize],
//              equal_to_results: &mut BooleanBufferBuilder| {
//                 let iter = lhs_rows.iter().zip(rhs_rows.iter());
//                 for (idx, (&lhs_row, &rhs_row)) in iter.enumerate() {
//                     equal_to_results
//                         .set_bit(idx, builder.equal_to(lhs_row, input_array, rhs_row));
//                 }
//             };
//
//         test_byte_equal_to_internal(append, equal_to);
//     }
//
//     #[test]
//     fn test_byte_vectorized_equal_to() {
//         let append = |builder: &mut ByteGroupValueBuilder<i32>,
//                       builder_array: &ArrayRef,
//                       append_rows: &[usize]| {
//             builder
//                 .vectorized_append(builder_array, append_rows)
//                 .unwrap();
//         };
//
//         let equal_to =
//             |builder: &ByteGroupValueBuilder<i32>,
//              lhs_rows: &[usize],
//              input_array: &ArrayRef,
//              rhs_rows: &[usize],
//              equal_to_results: &mut BooleanBufferBuilder| {
//                 builder.vectorized_equal_to(
//                     lhs_rows,
//                     input_array,
//                     rhs_rows,
//                     equal_to_results,
//                 );
//             };
//
//         test_byte_equal_to_internal(append, equal_to);
//     }
//
//     #[test]
//     fn test_byte_vectorized_operation_special_case() {
//         // Test the special `all nulls` or `not nulls` input array case
//         // for vectorized append and equal to
//
//         let mut builder = ByteGroupValueBuilder::<i32>::new(OutputType::Utf8);
//
//         // All nulls input array
//         let all_nulls_input_array = Arc::new(StringArray::from(vec![
//             Option::<&str>::None,
//             None,
//             None,
//             None,
//             None,
//         ])) as _;
//         builder
//             .vectorized_append(&all_nulls_input_array, &[0, 1, 2, 3, 4])
//             .unwrap();
//
//         let mut equal_to_results = make_true_buffer(all_nulls_input_array.len());
//         builder.vectorized_equal_to(
//             &[0, 1, 2, 3, 4],
//             &all_nulls_input_array,
//             &[0, 1, 2, 3, 4],
//             &mut equal_to_results,
//         );
//         let results = to_vec(&equal_to_results);
//
//         assert!(results[0]);
//         assert!(results[1]);
//         assert!(results[2]);
//         assert!(results[3]);
//         assert!(results[4]);
//
//         // All not nulls input array
//         let all_not_nulls_input_array = Arc::new(StringArray::from(vec![
//             Some("string1"),
//             Some("string2"),
//             Some("string3"),
//             Some("string4"),
//             Some("string5"),
//         ])) as _;
//         builder
//             .vectorized_append(&all_not_nulls_input_array, &[0, 1, 2, 3, 4])
//             .unwrap();
//
//         let mut equal_to_results = make_true_buffer(all_not_nulls_input_array.len());
//         builder.vectorized_equal_to(
//             &[5, 6, 7, 8, 9],
//             &all_not_nulls_input_array,
//             &[0, 1, 2, 3, 4],
//             &mut equal_to_results,
//         );
//         let results = to_vec(&equal_to_results);
//
//         assert!(results[0]);
//         assert!(results[1]);
//         assert!(results[2]);
//         assert!(results[3]);
//         assert!(results[4]);
//     }
//
//     fn test_byte_equal_to_internal<A, E>(mut append: A, mut equal_to: E)
//     where
//         A: FnMut(&mut ByteGroupValueBuilder<i32>, &ArrayRef, &[usize]),
//         E: FnMut(
//             &ByteGroupValueBuilder<i32>,
//             &[usize],
//             &ArrayRef,
//             &[usize],
//             &mut BooleanBufferBuilder,
//         ),
//     {
//         // Will cover such cases:
//         //   - exist null, input not null
//         //   - exist null, input null; values not equal
//         //   - exist null, input null; values equal
//         //   - exist not null, input null
//         //   - exist not null, input not null; values not equal
//         //   - exist not null, input not null; values equal
//
//         // Define ByteGroupValueBuilder
//         let mut builder = ByteGroupValueBuilder::<i32>::new(OutputType::Utf8);
//         let builder_array = Arc::new(StringArray::from(vec![
//             None,
//             None,
//             None,
//             Some("foo"),
//             Some("bar"),
//             Some("baz"),
//         ])) as ArrayRef;
//         append(&mut builder, &builder_array, &[0, 1, 2, 3, 4, 5]);
//
//         // Define input array
//         let (offsets, buffer, _nulls) = StringArray::from(vec![
//             Some("foo"),
//             Some("bar"),
//             None,
//             None,
//             Some("foo"),
//             Some("baz"),
//         ])
//         .into_parts();
//
//         // explicitly build a null buffer where one of the null values also happens to match
//         let mut nulls = NullBufferBuilder::new(6);
//         nulls.append_non_null();
//         nulls.append_null();
//         nulls.append_null();
//         nulls.append_null();
//         nulls.append_non_null();
//         nulls.append_non_null();
//         let input_array =
//             Arc::new(StringArray::new(offsets, buffer, nulls.finish())) as ArrayRef;
//
//         // Check
//         let mut equal_to_results = make_true_buffer(builder.len());
//         equal_to(
//             &builder,
//             &[0, 1, 2, 3, 4, 5],
//             &input_array,
//             &[0, 1, 2, 3, 4, 5],
//             &mut equal_to_results,
//         );
//         let results = to_vec(&equal_to_results);
//
//         assert!(!results[0]);
//         assert!(results[1]);
//         assert!(results[2]);
//         assert!(!results[3]);
//         assert!(!results[4]);
//         assert!(results[5]);
//     }
// }
