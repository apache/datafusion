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

//! Optimized filters for fixed-size binary IN lists.

use std::hash::Hash;
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, FixedSizeBinaryArray, PrimitiveArray,
};
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Decimal128Type, UInt8Type, UInt16Type, UInt32Type,
    UInt64Type,
};
use datafusion_common::{Result, exec_datafusion_err};

use super::primitive_filter::{
    BitmapFilter, BitmapFilterType, PrimitiveFilterNative, PrimitiveFilterType,
    PrimitiveFrozenFilter, branchless_filter_for_len,
};
use super::static_filter::{StaticFilter, handle_dictionary};

type StaticFilterRef = Arc<dyn StaticFilter + Send + Sync>;

/// Reinterpret fixed-size binary values as same-width primitive values.
///
/// Arrow buffers are normally sufficiently aligned, making this zero-copy. A
/// valid Arrow array can still be constructed from a sliced, unaligned buffer;
/// in that case, copy each value into aligned primitive storage.
fn reinterpret_fixed_size_binary<T>(
    array: &FixedSizeBinaryArray,
) -> Result<PrimitiveArray<T>>
where
    T: ArrowPrimitiveType,
{
    let width = size_of::<T::Native>();
    if usize::try_from(array.value_length()).ok() != Some(width) {
        return Err(exec_datafusion_err!(
            "FixedSizeBinary filter: expected {width}-byte values, got {}",
            array.value_length()
        ));
    }

    let source = array.values();
    let values = if source.as_ptr().align_offset(align_of::<T::Native>()) == 0 {
        ScalarBuffer::new(source.clone(), 0, array.len())
    } else {
        let mut values = Vec::with_capacity(array.len());
        for index in 0..array.len() {
            // SAFETY: `FixedSizeBinaryArray` guarantees that `source` contains
            // at least `array.len() * width` bytes. Arrow native values are
            // trivially transmutable, and `read_unaligned` accepts this pointer.
            let value = unsafe {
                source
                    .as_ptr()
                    .add(index * width)
                    .cast::<T::Native>()
                    .read_unaligned()
            };
            values.push(value);
        }
        ScalarBuffer::from(values)
    };

    Ok(PrimitiveArray::<T>::new(values, array.nulls().cloned()))
}

struct FixedSizeBinaryFilter<T: PrimitiveFilterType> {
    data_type: DataType,
    inner: StaticFilterRef,
    _marker: PhantomData<T>,
}

impl<T: PrimitiveFilterType> FixedSizeBinaryFilter<T> {
    fn new(data_type: DataType, inner: StaticFilterRef) -> Self {
        Self {
            data_type,
            inner,
            _marker: PhantomData,
        }
    }
}

impl<T> StaticFilter for FixedSizeBinaryFilter<T>
where
    T: PrimitiveFilterType,
{
    fn null_count(&self) -> usize {
        self.inner.null_count()
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        handle_dictionary!(self, v, negated);

        if v.data_type() != &self.data_type {
            return Err(exec_datafusion_err!(
                "FixedSizeBinary filter: expected {} array, got {}",
                self.data_type,
                v.data_type()
            ));
        }
        let array = v.as_fixed_size_binary_opt().ok_or_else(|| {
            exec_datafusion_err!(
                "FixedSizeBinary filter: expected concrete {} array",
                self.data_type
            )
        })?;
        let primitive: ArrayRef = Arc::new(reinterpret_fixed_size_binary::<T>(array)?);
        self.inner.contains(primitive.as_ref(), negated)
    }
}

fn downcast_fixed_size_binary(in_array: &ArrayRef) -> Result<&FixedSizeBinaryArray> {
    in_array.as_fixed_size_binary_opt().ok_or_else(|| {
        exec_datafusion_err!(
            "FixedSizeBinary filter: expected concrete fixed-size binary array, got {}",
            in_array.data_type()
        )
    })
}

fn bitmap_filter<T>(in_array: &ArrayRef) -> Result<StaticFilterRef>
where
    T: BitmapFilterType + PrimitiveFilterType,
{
    let array = downcast_fixed_size_binary(in_array)?;
    let primitive: ArrayRef = Arc::new(reinterpret_fixed_size_binary::<T>(array)?);
    let inner = Arc::new(BitmapFilter::<T>::try_new(&primitive)?);
    Ok(Arc::new(FixedSizeBinaryFilter::<T>::new(
        in_array.data_type().clone(),
        inner,
    )))
}

fn wide_filter<T>(in_array: &ArrayRef) -> Result<StaticFilterRef>
where
    T: PrimitiveFilterType,
    PrimitiveFilterNative<T>: Copy + Eq + Hash + Send + Sync,
{
    let array = downcast_fixed_size_binary(in_array)?;
    let primitive: ArrayRef = Arc::new(reinterpret_fixed_size_binary::<T>(array)?);
    let non_null_count = primitive.len() - primitive.null_count();
    let inner = if non_null_count <= T::BRANCHLESS_MAX_LIST_LEN {
        branchless_filter_for_len::<T>(&primitive, non_null_count)?
    } else {
        Arc::new(PrimitiveFrozenFilter::<T>::try_new(&primitive)?)
    };
    Ok(Arc::new(FixedSizeBinaryFilter::<T>::new(
        in_array.data_type().clone(),
        inner,
    )))
}

pub(super) fn instantiate_fixed_size_binary_filter(
    in_array: &ArrayRef,
) -> Result<Option<StaticFilterRef>> {
    let DataType::FixedSizeBinary(width) = in_array.data_type() else {
        return Ok(None);
    };
    if in_array.as_fixed_size_binary_opt().is_none() {
        return Ok(None);
    }

    let filter = match width {
        1 => bitmap_filter::<UInt8Type>(in_array)?,
        2 => bitmap_filter::<UInt16Type>(in_array)?,
        4 => wide_filter::<UInt32Type>(in_array)?,
        8 => wide_filter::<UInt64Type>(in_array)?,
        16 => wide_filter::<Decimal128Type>(in_array)?,
        _ => return Ok(None),
    };
    Ok(Some(filter))
}

#[cfg(test)]
mod tests {
    use std::any::Any;

    use arrow::array::{ArrayData, DictionaryArray, Int8Array};
    use arrow::buffer::{Buffer, NullBuffer};
    use arrow::datatypes::Int8Type;

    use super::*;

    #[derive(Clone, Debug)]
    struct CustomFixedSizeBinaryArray(FixedSizeBinaryArray);

    // SAFETY: Every Array method delegates to the wrapped, valid Arrow array.
    unsafe impl Array for CustomFixedSizeBinaryArray {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn to_data(&self) -> ArrayData {
            self.0.to_data()
        }

        fn into_data(self) -> ArrayData {
            self.0.into_data()
        }

        fn data_type(&self) -> &DataType {
            self.0.data_type()
        }

        fn slice(&self, offset: usize, length: usize) -> ArrayRef {
            Arc::new(Self(self.0.slice(offset, length)))
        }

        fn len(&self) -> usize {
            self.0.len()
        }

        fn is_empty(&self) -> bool {
            self.0.is_empty()
        }

        fn offset(&self) -> usize {
            self.0.offset()
        }

        fn nulls(&self) -> Option<&NullBuffer> {
            self.0.nulls()
        }

        fn get_buffer_memory_size(&self) -> usize {
            self.0.get_buffer_memory_size()
        }

        fn get_array_memory_size(&self) -> usize {
            self.0.get_array_memory_size()
        }
    }

    fn value(width: i32, index: usize, miss: bool) -> Vec<u8> {
        let mut value = (index as u128).to_le_bytes()[..width as usize].to_vec();
        let last = value.last_mut().unwrap();
        if miss {
            *last |= 0x80;
        } else {
            *last &= 0x7f;
        }
        value
    }

    fn array(width: i32, values: &[Option<Vec<u8>>]) -> FixedSizeBinaryArray {
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            values.iter().map(|value| value.as_deref()),
            width,
        )
        .unwrap()
    }

    fn make_filter(width: i32, values: &[Option<Vec<u8>>]) -> Result<StaticFilterRef> {
        let in_array: ArrayRef = Arc::new(array(width, values));
        Ok(instantiate_fixed_size_binary_filter(&in_array)?.unwrap())
    }

    #[test]
    fn filters_supported_widths_across_strategy_thresholds() -> Result<()> {
        for (width, list_len) in [
            (1, 16),
            (2, 64),
            (4, 32),
            (4, 33),
            (8, 16),
            (8, 17),
            (16, 4),
            (16, 5),
        ] {
            let haystack = (0..list_len)
                .map(|index| Some(value(width, index, false)))
                .collect::<Vec<_>>();
            let filter = make_filter(width, &haystack)?;
            let needles = array(
                width,
                &[
                    Some(value(width, list_len / 2, false)),
                    Some(value(width, list_len, true)),
                    None,
                ],
            );
            assert_eq!(
                filter.contains(&needles, false)?,
                BooleanArray::from(vec![Some(true), Some(false), None]),
                "width={width}, list_len={list_len}"
            );
        }
        Ok(())
    }

    #[test]
    fn handles_slices_nulls_and_not_in() -> Result<()> {
        let width = 8;
        let parent = array(
            width,
            &[
                Some(value(width, 1, false)),
                None,
                Some(value(width, 2, false)),
            ],
        );
        let in_array: ArrayRef = Arc::new(parent.slice(1, 2));
        let filter = instantiate_fixed_size_binary_filter(&in_array)?.unwrap();
        let needles = array(
            width,
            &[
                Some(value(width, 2, false)),
                Some(value(width, 3, false)),
                None,
            ],
        );

        assert_eq!(
            filter.contains(&needles, false)?,
            BooleanArray::from(vec![Some(true), None, None])
        );
        assert_eq!(
            filter.contains(&needles, true)?,
            BooleanArray::from(vec![Some(false), None, None])
        );
        Ok(())
    }

    #[test]
    fn handles_empty_all_null_duplicate_and_sentinel_lists() -> Result<()> {
        let empty = make_filter(4, &[])?;
        let needle = array(4, &[Some(value(4, 0, false))]);
        assert_eq!(
            empty.contains(&needle, false)?,
            BooleanArray::from(vec![Some(false)])
        );

        let all_null = make_filter(4, &vec![None; 40])?;
        assert_eq!(
            all_null.contains(&needle, false)?,
            BooleanArray::from(vec![None])
        );

        // More than 32 values selects FrozenSet. Keep the first real member at
        // zero and include duplicates to exercise its sentinel representation.
        let mut haystack = vec![Some(value(4, 0, false)); 2];
        haystack.extend((1..32).map(|index| Some(value(4, index, false))));
        let filter = make_filter(4, &haystack)?;
        let needles = array(4, &[Some(value(4, 0, false)), Some(value(4, 40, true))]);
        assert_eq!(
            filter.contains(&needles, false)?,
            BooleanArray::from(vec![Some(true), Some(false)])
        );
        Ok(())
    }

    #[test]
    fn handles_dictionary_needles() -> Result<()> {
        let filter = make_filter(4, &[Some(value(4, 7, false))])?;
        let dictionary_values: ArrayRef = Arc::new(array(
            4,
            &[Some(value(4, 7, false)), Some(value(4, 8, false))],
        ));
        let keys = Int8Array::from(vec![Some(0), Some(1), None]);
        let needles =
            DictionaryArray::<Int8Type>::try_new(keys, dictionary_values).unwrap();

        assert_eq!(
            filter.contains(&needles, false)?,
            BooleanArray::from(vec![Some(true), Some(false), None])
        );
        Ok(())
    }

    #[test]
    fn rejects_width_mismatch_and_unsupported_widths() -> Result<()> {
        let filter = make_filter(4, &[Some(value(4, 1, false))])?;
        let wrong_width = array(8, &[Some(value(8, 1, false))]);
        assert!(filter.contains(&wrong_width, false).is_err());

        let unsupported: ArrayRef = Arc::new(array(3, &[Some(vec![1, 2, 3])]));
        assert!(instantiate_fixed_size_binary_filter(&unsupported)?.is_none());
        Ok(())
    }

    #[test]
    fn custom_haystacks_fall_back_and_custom_needles_error() -> Result<()> {
        let concrete = array(4, &[Some(value(4, 1, false))]);
        let custom: ArrayRef = Arc::new(CustomFixedSizeBinaryArray(concrete.clone()));
        assert!(instantiate_fixed_size_binary_filter(&custom)?.is_none());

        let filter = make_filter(4, &[Some(value(4, 1, false))])?;
        assert!(filter.contains(custom.as_ref(), false).is_err());
        Ok(())
    }

    #[test]
    fn aligned_buffers_are_reinterpreted_zero_copy() -> Result<()> {
        let buffer = Buffer::from_vec(vec![1_u64, 2, 3]);
        let source_ptr = buffer.as_ptr();
        let array = FixedSizeBinaryArray::new(8, buffer, None);
        let primitive = reinterpret_fixed_size_binary::<UInt64Type>(&array)?;

        assert_eq!(primitive.values().inner().as_ptr(), source_ptr);
        Ok(())
    }

    #[test]
    fn unaligned_haystacks_and_needles_are_copied_safely() -> Result<()> {
        fn unaligned_array(width: i32, values: &[Vec<u8>]) -> FixedSizeBinaryArray {
            let mut bytes = vec![0];
            bytes.extend(values.iter().flatten());
            let buffer = Buffer::from(bytes).slice(1);
            assert_ne!(
                buffer.as_ptr().align_offset(width as usize),
                0,
                "test buffer must be unaligned"
            );
            FixedSizeBinaryArray::new(width, buffer, None)
        }

        let width = 16;
        let haystack_values = (0..5)
            .map(|index| value(width, index, false))
            .collect::<Vec<_>>();
        let haystack: ArrayRef = Arc::new(unaligned_array(width, &haystack_values));
        let filter = instantiate_fixed_size_binary_filter(&haystack)?.unwrap();
        let needles =
            unaligned_array(width, &[value(width, 3, false), value(width, 9, true)]);

        assert_eq!(
            filter.contains(&needles, false)?,
            BooleanArray::from(vec![Some(true), Some(false)])
        );
        Ok(())
    }

    #[test]
    fn preserves_nulls_when_copying_unaligned_values() -> Result<()> {
        let width = 8;
        let mut bytes = vec![0];
        bytes.extend(value(width, 1, false));
        bytes.extend(value(width, 2, false));
        let buffer = Buffer::from(bytes).slice(1);
        let nulls = NullBuffer::from(vec![true, false]);
        let array = FixedSizeBinaryArray::new(width, buffer, Some(nulls.clone()));
        let primitive = reinterpret_fixed_size_binary::<UInt64Type>(&array)?;

        assert_eq!(primitive.nulls(), Some(&nulls));
        Ok(())
    }
}
