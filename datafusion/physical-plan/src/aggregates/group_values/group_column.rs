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

use arrow::array::BufferBuilder;
use arrow::array::GenericBinaryArray;
use arrow::array::GenericStringArray;
use arrow::array::OffsetSizeTrait;
use arrow::array::PrimitiveArray;
use arrow::array::{Array, ArrayRef, ArrowPrimitiveType, AsArray};
use arrow::buffer::OffsetBuffer;
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::ByteArrayType;
use arrow::datatypes::DataType;
use arrow::datatypes::GenericBinaryType;
use datafusion_common::utils::proxy::VecAllocExt;

use crate::aggregates::group_values::null_builder::MaybeNullBufferBuilder;
use arrow_array::types::GenericStringType;
use datafusion_physical_expr_common::binary_map::{OutputType, INITIAL_BUFFER_CAPACITY};
use std::sync::Arc;
use std::vec;

/// Trait for storing a single column of group values in [`GroupValuesColumn`]
///
/// Implementations of this trait store an in-progress collection of group values
/// (similar to various builders in Arrow-rs) that allow for quick comparison to
/// incoming rows.
///
/// [`GroupValuesColumn`]: crate::aggregates::group_values::GroupValuesColumn
pub trait GroupColumn: Send + Sync {
    /// Returns equal if the row stored in this builder at `lhs_row` is equal to
    /// the row in `array` at `rhs_row`
    ///
    /// Note that this comparison returns true if both elements are NULL
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool;
    /// Appends the row at `row` in `array` to this builder
    fn append_val(&mut self, array: &ArrayRef, row: usize);
    /// Returns the number of rows stored in this builder
    fn len(&self) -> usize;
    /// Returns the number of bytes used by this [`GroupColumn`]
    fn size(&self) -> usize;
    /// Builds a new array from all of the stored rows
    fn build(self: Box<Self>) -> ArrayRef;
    /// Builds a new array from the first `n` stored rows, shifting the
    /// remaining rows to the start of the builder
    fn take_n(&mut self, n: usize) -> ArrayRef;
}

/// An implementation of [`GroupColumn`] for primitive values
///
/// Optimized to skip null buffer construction if the input is known to be non nullable
///
/// # Template parameters
///
/// `T`: the native Rust type that stores the data
/// `NULLABLE`: if the data can contain any nulls
#[derive(Debug)]
pub struct PrimitiveGroupValueBuilder<T: ArrowPrimitiveType, const NULLABLE: bool> {
    group_values: Vec<T::Native>,
    nulls: MaybeNullBufferBuilder,
}

impl<T, const NULLABLE: bool> PrimitiveGroupValueBuilder<T, NULLABLE>
where
    T: ArrowPrimitiveType,
{
    /// Create a new `PrimitiveGroupValueBuilder`
    pub fn new() -> Self {
        Self {
            group_values: vec![],
            nulls: MaybeNullBufferBuilder::new(),
        }
    }
}

impl<T: ArrowPrimitiveType, const NULLABLE: bool> GroupColumn
    for PrimitiveGroupValueBuilder<T, NULLABLE>
{
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool {
        // Perf: skip null check (by short circuit) if input is not nullable
        if NULLABLE {
            let exist_null = self.nulls.is_null(lhs_row);
            let input_null = array.is_null(rhs_row);
            if let Some(result) = nulls_equal_to(exist_null, input_null) {
                return result;
            }
            // Otherwise, we need to check their values
        }

        self.group_values[lhs_row] == array.as_primitive::<T>().value(rhs_row)
    }

    fn append_val(&mut self, array: &ArrayRef, row: usize) {
        // Perf: skip null check if input can't have nulls
        if NULLABLE {
            if array.is_null(row) {
                self.nulls.append(true);
                self.group_values.push(T::default_value());
            } else {
                self.nulls.append(false);
                self.group_values.push(array.as_primitive::<T>().value(row));
            }
        } else {
            self.group_values.push(array.as_primitive::<T>().value(row));
        }
    }

    fn len(&self) -> usize {
        self.group_values.len()
    }

    fn size(&self) -> usize {
        self.group_values.allocated_size() + self.nulls.allocated_size()
    }

    fn build(self: Box<Self>) -> ArrayRef {
        let Self {
            group_values,
            nulls,
        } = *self;

        let nulls = nulls.build();
        if !NULLABLE {
            assert!(nulls.is_none(), "unexpected nulls in non nullable input");
        }

        Arc::new(PrimitiveArray::<T>::new(
            ScalarBuffer::from(group_values),
            nulls,
        ))
    }

    fn take_n(&mut self, n: usize) -> ArrayRef {
        let first_n = self.group_values.drain(0..n).collect::<Vec<_>>();

        let first_n_nulls = if NULLABLE { self.nulls.take_n(n) } else { None };

        Arc::new(PrimitiveArray::<T>::new(
            ScalarBuffer::from(first_n),
            first_n_nulls,
        ))
    }
}

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
        }
    }

    fn append_val_inner<B>(&mut self, array: &ArrayRef, row: usize)
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
            let value: &[u8] = arr.value(row).as_ref();
            self.buffer.append_slice(value);
            self.offsets.push(O::usize_as(self.buffer.len()));
        }
    }

    fn equal_to_inner<B>(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool
    where
        B: ByteArrayType,
    {
        let array = array.as_bytes::<B>();
        let exist_null = self.nulls.is_null(lhs_row);
        let input_null = array.is_null(rhs_row);
        if let Some(result) = nulls_equal_to(exist_null, input_null) {
            return result;
        }
        // Otherwise, we need to check their values
        self.value(lhs_row) == (array.value(rhs_row).as_ref() as &[u8])
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

    fn append_val(&mut self, column: &ArrayRef, row: usize) {
        // Sanity array type
        match self.output_type {
            OutputType::Binary => {
                debug_assert!(matches!(
                    column.data_type(),
                    DataType::Binary | DataType::LargeBinary
                ));
                self.append_val_inner::<GenericBinaryType<O>>(column, row)
            }
            OutputType::Utf8 => {
                debug_assert!(matches!(
                    column.data_type(),
                    DataType::Utf8 | DataType::LargeUtf8
                ));
                self.append_val_inner::<GenericStringType<O>>(column, row)
            }
            _ => unreachable!("View types should use `ArrowBytesViewMap`"),
        };
    }

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn size(&self) -> usize {
        self.buffer.capacity() * std::mem::size_of::<u8>()
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

        // Given offests like [0, 2, 4, 5] and n = 1, we expect to get
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

/// Determines if the nullability of the existing and new input array can be used
/// to short-circuit the comparison of the two values.
///
/// Returns `Some(result)` if the result of the comparison can be determined
/// from the nullness of the two values, and `None` if the comparison must be
/// done on the values themselves.
fn nulls_equal_to(lhs_null: bool, rhs_null: bool) -> Option<bool> {
    match (lhs_null, rhs_null) {
        (true, true) => Some(true),
        (false, true) | (true, false) => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::Int64Type;
    use arrow_array::{ArrayRef, Int64Array, StringArray};
    use arrow_buffer::{BooleanBufferBuilder, NullBuffer};
    use datafusion_physical_expr::binary_map::OutputType;

    use crate::aggregates::group_values::group_column::PrimitiveGroupValueBuilder;

    use super::{ByteGroupValueBuilder, GroupColumn};

    #[test]
    fn test_take_n() {
        let mut builder = ByteGroupValueBuilder::<i32>::new(OutputType::Utf8);
        let array = Arc::new(StringArray::from(vec![Some("a"), None])) as ArrayRef;
        // a, null, null
        builder.append_val(&array, 0);
        builder.append_val(&array, 1);
        builder.append_val(&array, 1);

        // (a, null) remaining: null
        let output = builder.take_n(2);
        assert_eq!(&output, &array);

        // null, a, null, a
        builder.append_val(&array, 0);
        builder.append_val(&array, 1);
        builder.append_val(&array, 0);

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
        builder.append_val(&array, 2);
        builder.append_val(&array, 1);
        builder.append_val(&array, 1);

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
    fn test_nullable_primitive_equal_to() {
        // Will cover such cases:
        //   - exist null, input not null
        //   - exist null, input null; values not equal
        //   - exist null, input null; values equal
        //   - exist not null, input null
        //   - exist not null, input not null; values not equal
        //   - exist not null, input not null; values equal

        // Define PrimitiveGroupValueBuilder
        let mut builder = PrimitiveGroupValueBuilder::<Int64Type, true>::new();
        let builder_array = Arc::new(Int64Array::from(vec![
            None,
            None,
            None,
            Some(1),
            Some(2),
            Some(3),
        ])) as ArrayRef;
        builder.append_val(&builder_array, 0);
        builder.append_val(&builder_array, 1);
        builder.append_val(&builder_array, 2);
        builder.append_val(&builder_array, 3);
        builder.append_val(&builder_array, 4);
        builder.append_val(&builder_array, 5);

        // Define input array
        let (_nulls, values, _) =
            Int64Array::from(vec![Some(1), Some(2), None, None, Some(1), Some(3)])
                .into_parts();

        // explicitly build a boolean buffer where one of the null values also happens to match
        let mut boolean_buffer_builder = BooleanBufferBuilder::new(6);
        boolean_buffer_builder.append(true);
        boolean_buffer_builder.append(false); // this sets Some(2) to null above
        boolean_buffer_builder.append(false);
        boolean_buffer_builder.append(false);
        boolean_buffer_builder.append(true);
        boolean_buffer_builder.append(true);
        let nulls = NullBuffer::new(boolean_buffer_builder.finish());
        let input_array = Arc::new(Int64Array::new(values, Some(nulls))) as ArrayRef;

        // Check
        assert!(!builder.equal_to(0, &input_array, 0));
        assert!(builder.equal_to(1, &input_array, 1));
        assert!(builder.equal_to(2, &input_array, 2));
        assert!(!builder.equal_to(3, &input_array, 3));
        assert!(!builder.equal_to(4, &input_array, 4));
        assert!(builder.equal_to(5, &input_array, 5));
    }

    #[test]
    fn test_not_nullable_primitive_equal_to() {
        // Will cover such cases:
        //   - values equal
        //   - values not equal

        // Define PrimitiveGroupValueBuilder
        let mut builder = PrimitiveGroupValueBuilder::<Int64Type, false>::new();
        let builder_array =
            Arc::new(Int64Array::from(vec![Some(0), Some(1)])) as ArrayRef;
        builder.append_val(&builder_array, 0);
        builder.append_val(&builder_array, 1);

        // Define input array
        let input_array = Arc::new(Int64Array::from(vec![Some(0), Some(2)])) as ArrayRef;

        // Check
        assert!(builder.equal_to(0, &input_array, 0));
        assert!(!builder.equal_to(1, &input_array, 1));
    }

    #[test]
    fn test_byte_array_equal_to() {
        // Will cover such cases:
        //   - exist null, input not null
        //   - exist null, input null; values not equal
        //   - exist null, input null; values equal
        //   - exist not null, input null
        //   - exist not null, input not null; values not equal
        //   - exist not null, input not null; values equal

        // Define PrimitiveGroupValueBuilder
        let mut builder = ByteGroupValueBuilder::<i32>::new(OutputType::Utf8);
        let builder_array = Arc::new(StringArray::from(vec![
            None,
            None,
            None,
            Some("foo"),
            Some("bar"),
            Some("baz"),
        ])) as ArrayRef;
        builder.append_val(&builder_array, 0);
        builder.append_val(&builder_array, 1);
        builder.append_val(&builder_array, 2);
        builder.append_val(&builder_array, 3);
        builder.append_val(&builder_array, 4);
        builder.append_val(&builder_array, 5);

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
        let mut boolean_buffer_builder = BooleanBufferBuilder::new(6);
        boolean_buffer_builder.append(true);
        boolean_buffer_builder.append(false); // this sets Some("bar") to null above
        boolean_buffer_builder.append(false);
        boolean_buffer_builder.append(false);
        boolean_buffer_builder.append(true);
        boolean_buffer_builder.append(true);
        let nulls = NullBuffer::new(boolean_buffer_builder.finish());
        let input_array =
            Arc::new(StringArray::new(offsets, buffer, Some(nulls))) as ArrayRef;

        // Check
        assert!(!builder.equal_to(0, &input_array, 0));
        assert!(builder.equal_to(1, &input_array, 1));
        assert!(builder.equal_to(2, &input_array, 2));
        assert!(!builder.equal_to(3, &input_array, 3));
        assert!(!builder.equal_to(4, &input_array, 4));
        assert!(builder.equal_to(5, &input_array, 5));
    }
}
