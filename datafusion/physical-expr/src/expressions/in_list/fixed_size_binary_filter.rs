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

//! Optimized filters for fixed-size binary `IN` lists.
//!
//! Supported widths use an Arrow primitive representation with the same
//! in-memory size:
//!
//! | Width | Primitive representation |
//! |------:|--------------------------|
//! | 1     | `UInt8`                  |
//! | 2     | `UInt16`                 |
//! | 4     | `UInt32`                 |
//! | 8     | `UInt64`                 |
//! | 16    | `Decimal128`             |
//!
//! The shared primitive selector applies the native primitive branchless cutoffs
//! and chooses the bitmap or hash-set fallback.
//!
//! The list and input bytes are read the same way, so each primitive value is
//! an exact key for comparison, bitmap lookup, or hashing. No arithmetic,
//! ordering, or decimal operations are used.
//!
//! Reinterpreting an aligned Arrow buffer is zero-copy. An unaligned buffer is
//! copied into aligned primitive storage before filter construction or probing.

use std::mem::size_of;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, FixedSizeBinaryArray, PrimitiveArray,
};
use arrow::buffer::{Buffer, ScalarBuffer};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Decimal128Type, UInt8Type, UInt16Type, UInt32Type,
    UInt64Type,
};
use datafusion_common::{Result, exec_datafusion_err, internal_datafusion_err};

use super::primitive_filter::instantiate_primitive_filter;
use super::static_filter::{StaticFilter, StaticFilterRef};

/// Reinterpret fixed-size binary values as same-width primitive values.
///
/// Arrow buffers are normally sufficiently aligned, making this zero-copy. A
/// valid Arrow array can still be constructed from a sliced, unaligned buffer;
/// in that case, copy each value into aligned primitive storage.
fn reinterpret_as_primitive<T>(array: &FixedSizeBinaryArray) -> Result<PrimitiveArray<T>>
where
    T: ArrowPrimitiveType,
{
    let width = size_of::<T::Native>();
    if array.value_size() != width {
        return Err(internal_datafusion_err!(
            "FixedSizeBinary filter: expected {width}-byte values, got {}",
            array.value_size()
        ));
    }

    let source = array.values();
    let values = if source.as_ptr().cast::<T::Native>().is_aligned() {
        ScalarBuffer::new(source.clone(), 0, array.len())
    } else {
        // `Buffer::from(&[u8])` copies into Arrow-aligned storage.
        ScalarBuffer::new(Buffer::from(source.as_slice()), 0, array.len())
    };

    Ok(PrimitiveArray::<T>::new(values, array.nulls().cloned()))
}

/// Adapts a primitive filter to concrete, same-width `FixedSizeBinary` arrays.
struct FixedSizeBinaryFilter {
    data_type: DataType,
    inner: StaticFilterRef,
}

impl StaticFilter for FixedSizeBinaryFilter {
    fn null_count(&self) -> usize {
        self.inner.null_count()
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
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
        let primitive = reinterpret(array)?.ok_or_else(|| {
            // Instantiation code rejects unsupported widths, so this is unexpected
            internal_datafusion_err!(
                "FixedSizeBinary filter: unsupported width {}",
                array.value_size()
            )
        })?;
        self.inner.contains(primitive.as_ref(), negated)
    }
}

/// Reinterprets a supported-width array as its same-width primitive array, or
/// `None` if the width has no primitive representation.
fn reinterpret(array: &FixedSizeBinaryArray) -> Result<Option<ArrayRef>> {
    Ok(Some(match array.value_size() {
        1 => Arc::new(reinterpret_as_primitive::<UInt8Type>(array)?) as ArrayRef,
        2 => Arc::new(reinterpret_as_primitive::<UInt16Type>(array)?),
        4 => Arc::new(reinterpret_as_primitive::<UInt32Type>(array)?),
        8 => Arc::new(reinterpret_as_primitive::<UInt64Type>(array)?),
        16 => Arc::new(reinterpret_as_primitive::<Decimal128Type>(array)?),

        _ => return Ok(None),
    }))
}

/// Creates an optimized filter for supported concrete `FixedSizeBinary` arrays.
pub(super) fn instantiate_fixed_size_binary_filter(
    in_array: &ArrayRef,
) -> Result<Option<StaticFilterRef>> {
    if !matches!(in_array.data_type(), DataType::FixedSizeBinary(_)) {
        return Ok(None);
    }
    let Some(array) = in_array.as_fixed_size_binary_opt() else {
        return Ok(None);
    };

    let Some(primitive) = reinterpret(array)? else {
        return Ok(None);
    };
    let inner = instantiate_primitive_filter(&primitive)?.ok_or_else(|| {
        // reinterpret should have returned None for unsupported widths, so this is unexpected
        internal_datafusion_err!(
            "FixedSizeBinary filter: no primitive filter for {}",
            primitive.data_type()
        )
    })?;
    Ok(Some(Arc::new(FixedSizeBinaryFilter {
        data_type: in_array.data_type().clone(),
        inner,
    })))
}

#[cfg(test)]
mod tests {
    use arrow::array::{DictionaryArray, Int8Array, StringArray};
    use arrow::buffer::{Buffer, MutableBuffer, NullBuffer};
    use arrow::datatypes::Int8Type;

    use super::super::dictionary_filter::DictionaryFilter;
    use super::*;

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
            (1, 17),
            (2, 8),
            (2, 9),
            (4, 32),
            (4, 33),
            (8, 16),
            (8, 17),
            (16, 4),
            (16, 5),
        ] {
            let mut hit = vec![0x80; width as usize];
            hit[width as usize - 1] = 0xff;
            let mut miss = hit.clone();
            miss[width as usize - 1] ^= 1;

            let mut haystack = (0..list_len - 1)
                .map(|index| Some(value(width, index, false)))
                .collect::<Vec<_>>();
            haystack.push(Some(hit.clone()));
            let filter = make_filter(width, &haystack)?;
            let needles = array(width, &[Some(hit), Some(miss), None]);
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
        let width = 16;
        let parent = array(
            width,
            &[
                Some(value(width, 0, false)),
                Some(value(width, 1, false)),
                None,
                Some(value(width, 2, false)),
                Some(value(width, 3, false)),
                Some(value(width, 4, false)),
                Some(value(width, 5, false)),
                Some(value(width, 6, false)),
            ],
        );
        // Five non-null values select the hash-set path.
        let in_array: ArrayRef = Arc::new(parent.slice(1, 6));
        let filter = instantiate_fixed_size_binary_filter(&in_array)?.unwrap();
        let needles = array(
            width,
            &[
                Some(value(width, 2, false)),
                Some(value(width, 0, false)),
                Some(value(width, 6, false)),
                Some(value(width, 7, false)),
                None,
            ],
        );

        assert_eq!(
            filter.contains(&needles, false)?,
            BooleanArray::from(vec![Some(true), None, None, None, None])
        );
        assert_eq!(
            filter.contains(&needles, true)?,
            BooleanArray::from(vec![Some(false), None, None, None, None])
        );
        Ok(())
    }

    #[test]
    fn handles_dictionary_needles() -> Result<()> {
        let filter = DictionaryFilter::new(make_filter(4, &[Some(value(4, 7, false))])?);
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
        assert_eq!(
            filter.contains(&needles, true)?,
            BooleanArray::from(vec![Some(false), Some(true), None])
        );
        Ok(())
    }

    #[test]
    fn rejects_unsupported_arrays() -> Result<()> {
        let filter = make_filter(4, &[Some(value(4, 1, false))])?;
        let wrong_width = array(8, &[Some(value(8, 1, false))]);
        let error = filter
            .contains(&wrong_width, false)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("expected FixedSizeBinary(4) array, got FixedSizeBinary(8)"),
            "{error}"
        );

        let wrong_type = StringArray::from(vec!["one"]);
        let error = filter.contains(&wrong_type, false).unwrap_err().to_string();
        assert!(
            error.contains("expected FixedSizeBinary(4) array, got Utf8"),
            "{error}"
        );

        for width in [0, 3, 5, 15, 17] {
            let unsupported: ArrayRef =
                Arc::new(FixedSizeBinaryArray::new_null(width, 1));
            assert!(
                instantiate_fixed_size_binary_filter(&unsupported)?.is_none(),
                "width={width}"
            );
        }

        Ok(())
    }

    // The cast only checks alignment; the pointer is never dereferenced.
    #[expect(clippy::cast_ptr_alignment)]
    fn unaligned_i128_array(
        values: &[Vec<u8>],
        nulls: Option<NullBuffer>,
    ) -> FixedSizeBinaryArray {
        let width = size_of::<i128>();
        let mut bytes = MutableBuffer::with_capacity(1 + width * values.len());
        bytes.push(0_u8);
        for value in values {
            assert_eq!(value.len(), width);
            bytes.extend_from_slice(value);
        }
        let buffer = Buffer::from(bytes).slice(1);
        assert!(
            !buffer.as_ptr().cast::<i128>().is_aligned(),
            "test buffer must be unaligned"
        );
        FixedSizeBinaryArray::new(width as i32, buffer, nulls)
    }

    #[test]
    fn handles_aligned_and_unaligned_buffers() -> Result<()> {
        let buffer = Buffer::from_vec(vec![1_u64, 2, 3]);
        let source_ptr = buffer.as_ptr();
        let array = FixedSizeBinaryArray::new(8, buffer, None);
        let primitive = reinterpret_as_primitive::<UInt64Type>(&array)?;
        assert_eq!(primitive.values().inner().as_ptr(), source_ptr);

        let width = 16;
        let haystack_values = (0..5)
            .map(|index| value(width, index, false))
            .collect::<Vec<_>>();
        let haystack: ArrayRef = Arc::new(unaligned_i128_array(&haystack_values, None));
        let needles = unaligned_i128_array(
            &[
                value(width, 3, false),
                value(width, 8, true),
                value(width, 9, true),
            ],
            Some(NullBuffer::from(vec![true, false, true])),
        );
        let filter = instantiate_fixed_size_binary_filter(&haystack)?.unwrap();

        assert_eq!(
            filter.contains(&needles, false)?,
            BooleanArray::from(vec![Some(true), None, Some(false)])
        );
        Ok(())
    }
}
