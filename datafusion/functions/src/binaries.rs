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

use crate::strings::{ColumnarValueRef, ConcatBuilder};
use arrow::array::{
    Array, ArrayDataBuilder, ArrayRef, BinaryViewArray, GenericBinaryArray,
    OffsetSizeTrait, make_view,
};
use arrow_buffer::{ArrowNativeType, Buffer, MutableBuffer, NullBuffer, ScalarBuffer};
use datafusion_common::{Result, exec_datafusion_err, exec_err, internal_err};
use std::marker::PhantomData;
use std::sync::Arc;

pub(crate) struct ConcatGenericBinaryBuilder<O: OffsetSizeTrait + ArrowNativeType> {
    offsets_buffer: MutableBuffer,
    value_buffer: MutableBuffer,
    _phantom: PhantomData<O>,
}
pub(crate) type ConcatBinaryBuilder = ConcatGenericBinaryBuilder<i32>;
pub(crate) type ConcatLargeBinaryBuilder = ConcatGenericBinaryBuilder<i64>;

impl<O: OffsetSizeTrait + ArrowNativeType> ConcatGenericBinaryBuilder<O> {
    pub fn with_capacity(item_capacity: usize, data_capacity: usize) -> Self {
        let capacity = item_capacity
            .checked_add(1)
            .map(|i| i.saturating_mul(size_of::<O>()))
            .expect("capacity integer overflow");

        let mut offsets_buffer = MutableBuffer::with_capacity(capacity);
        // SAFETY: the first offset value is definitely not going to exceed the bounds.
        unsafe { offsets_buffer.push_unchecked(O::usize_as(0)) };
        Self {
            offsets_buffer,
            value_buffer: MutableBuffer::with_capacity(data_capacity),
            _phantom: PhantomData,
        }
    }
}

impl<O: OffsetSizeTrait + ArrowNativeType> ConcatBuilder
    for ConcatGenericBinaryBuilder<O>
{
    fn write<const CHECK_VALID: bool>(
        &mut self,
        column: &ColumnarValueRef,
        i: usize,
    ) -> Result<()> {
        match column {
            ColumnarValueRef::Scalar(s) => {
                self.value_buffer.extend_from_slice(s);
            }
            ColumnarValueRef::NullableBinaryArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.value_buffer.extend_from_slice(array.value(i));
                }
            }
            ColumnarValueRef::NullableLargeBinaryArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.value_buffer.extend_from_slice(array.value(i));
                }
            }
            ColumnarValueRef::NullableBinaryViewArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.value_buffer.extend_from_slice(array.value(i));
                }
            }
            ColumnarValueRef::NonNullableBinaryArray(array) => {
                self.value_buffer.extend_from_slice(array.value(i));
            }
            ColumnarValueRef::NonNullableLargeBinaryArray(array) => {
                self.value_buffer.extend_from_slice(array.value(i));
            }
            ColumnarValueRef::NonNullableBinaryViewArray(array) => {
                self.value_buffer.extend_from_slice(array.value(i));
            }
            _ => {
                return exec_err!(
                    "concat: unexpected column type for binary builder: {column:?}"
                );
            }
        }
        Ok(())
    }

    fn append_offset(&mut self) -> Result<()> {
        let next_offset: O = O::from_usize(self.value_buffer.len())
            .ok_or_else(|| exec_datafusion_err!("byte array offset overflow"))?;
        self.offsets_buffer.push(next_offset);
        Ok(())
    }

    /// Finalize the builder into a concrete [`GenericBinaryArray<O>`].
    ///
    /// # Errors
    ///
    /// Returns an error when:
    ///
    /// - the provided `null_buffer` is not the same length as the `offsets_buffer`.
    fn finish(self, null_buffer: Option<NullBuffer>) -> Result<ArrayRef> {
        let row_count = self.offsets_buffer.len() / size_of::<O>() - 1;
        if let Some(ref null_buffer) = null_buffer
            && null_buffer.len() != row_count
        {
            return internal_err!(
                "Null buffer and offsets buffer must be the same length"
            );
        }
        let array_builder = ArrayDataBuilder::new(GenericBinaryArray::<O>::DATA_TYPE)
            .len(row_count)
            .add_buffer(self.offsets_buffer.into())
            .add_buffer(self.value_buffer.into())
            .nulls(null_buffer);
        // SAFETY: all data that was appended was valid and the values
        // and offsets were created correctly
        let array_data = unsafe { array_builder.build_unchecked() };
        let array = GenericBinaryArray::<O>::from(array_data);
        Ok(Arc::new(array))
    }
}

/// Builder used by `concat`/`concat_ws` to assemble a [`BinaryViewArray`] one
/// row at a time from multiple input columns.
///
/// Each row is written via repeated `write` calls (one per input
/// fragment) followed by a single `append_offset` to commit the row
/// as a single binary view. The output null buffer is supplied by the caller
/// at `finish` time, avoiding per-row NULL handling work.
///
pub(crate) struct ConcatBinaryViewBuilder {
    views: Vec<u128>,
    data: Vec<u8>,
    block: Vec<u8>,
}

impl ConcatBinaryViewBuilder {
    pub fn with_capacity(item_capacity: usize, data_capacity: usize) -> Self {
        Self {
            views: Vec::with_capacity(item_capacity),
            data: Vec::with_capacity(data_capacity),
            block: vec![],
        }
    }
}

impl ConcatBuilder for ConcatBinaryViewBuilder {
    fn write<const CHECK_VALID: bool>(
        &mut self,
        column: &ColumnarValueRef,
        i: usize,
    ) -> Result<()> {
        match column {
            ColumnarValueRef::Scalar(s) => {
                self.block.extend_from_slice(s);
            }
            ColumnarValueRef::NullableBinaryArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.block.extend_from_slice(array.value(i));
                }
            }
            ColumnarValueRef::NullableLargeBinaryArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.block.extend_from_slice(array.value(i));
                }
            }
            ColumnarValueRef::NullableBinaryViewArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.block.extend_from_slice(array.value(i));
                }
            }
            ColumnarValueRef::NonNullableBinaryArray(array) => {
                self.block.extend_from_slice(array.value(i));
            }
            ColumnarValueRef::NonNullableLargeBinaryArray(array) => {
                self.block.extend_from_slice(array.value(i));
            }
            ColumnarValueRef::NonNullableBinaryViewArray(array) => {
                self.block.extend_from_slice(array.value(i));
            }
            _ => {
                return exec_err!(
                    "concat: unexpected column type for binary view builder: {column:?}"
                );
            }
        }
        Ok(())
    }

    /// Finalizes the current row by converting the accumulated data into a
    /// StringView and appending it to the views buffer.
    fn append_offset(&mut self) -> Result<()> {
        let v = &self.block;
        if v.len() > 12 {
            let offset: u32 = self
                .data
                .len()
                .try_into()
                .map_err(|_| exec_datafusion_err!("byte array offset overflow"))?;
            self.data.extend_from_slice(v);
            self.views.push(make_view(v, 0, offset));
        } else {
            self.views.push(make_view(v, 0, 0));
        }

        self.block.clear();
        Ok(())
    }

    /// Finalize the builder into a concrete [`BinaryViewArray`].
    ///
    /// # Errors
    ///
    /// Returns an error when:
    ///
    /// - the provided `null_buffer` length does not match the row count.
    fn finish(self, null_buffer: Option<NullBuffer>) -> Result<ArrayRef> {
        if let Some(ref nulls) = null_buffer
            && nulls.len() != self.views.len()
        {
            return internal_err!(
                "Null buffer length ({}) must match row count ({})",
                nulls.len(),
                self.views.len()
            );
        }

        let buffers: Vec<Buffer> = if self.data.is_empty() {
            vec![]
        } else {
            vec![Buffer::from(self.data)]
        };

        // SAFETY: views were constructed with correct lengths, offsets, and
        // prefixes.
        let array = unsafe {
            BinaryViewArray::new_unchecked(
                ScalarBuffer::from(self.views),
                buffers,
                null_buffer,
            )
        };
        Ok(Arc::new(array))
    }
}
