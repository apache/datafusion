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
//! Supported widths use an Arrow primitive type with the same in-memory size:
//!
//! | Width | Primitive type | Branchless through | Larger lists |
//! |------:|----------------|-------------------:|--------------|
//! | 1     | `UInt8`        | 16 values          | bitmap       |
//! | 2     | `UInt16`       | 8 values           | bitmap       |
//! | 4     | `UInt32`       | 32 values          | hash set     |
//! | 8     | `UInt64`       | 16 values          | hash set     |
//! | 16    | `Decimal128`   | 4 values           | hash set     |
//!
//! The limits count non-null list values.
//!
//! The list and input bytes are read the same way, so each primitive value is
//! an exact key for comparison, bitmap lookup, or hashing. No arithmetic,
//! ordering, or decimal operations are used.
//!
//! Aligned Arrow buffers are reused without copying. Unaligned buffers are
//! copied into aligned primitive storage.

use std::hash::Hash;
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, FixedSizeBinaryArray, PrimitiveArray,
};
use arrow::buffer::{Buffer, ScalarBuffer};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Decimal128Type, UInt8Type, UInt16Type, UInt32Type,
    UInt64Type,
};
use datafusion_common::{HashSet, Result, exec_datafusion_err, internal_datafusion_err};

use super::branchless_filter::{
    BranchlessFilter, BranchlessFilterType, BranchlessNative,
};
use super::primitive_filter::{BitmapFilter, BitmapFilterType};
use super::result::build_in_list_result;
use super::static_filter::{StaticFilter, handle_dictionary};

type StaticFilterRef = Arc<dyn StaticFilter + Send + Sync>;

fn use_branchless<T: BranchlessFilterType>(count: usize) -> bool {
    count <= T::MAX_LIST_LEN
}

/// Reinterpret fixed-size binary values as same-width primitive values.
///
/// Arrow buffers are normally sufficiently aligned, making this zero-copy. A
/// valid Arrow array can still be constructed from a sliced, unaligned buffer;
/// in that case, copy each value into aligned primitive storage.
fn as_primitive<T>(array: &FixedSizeBinaryArray) -> Result<PrimitiveArray<T>>
where
    T: ArrowPrimitiveType,
{
    let width = size_of::<T::Native>();
    if usize::try_from(array.value_length()).ok() != Some(width) {
        return Err(internal_datafusion_err!(
            "FixedSizeBinary filter: expected {width}-byte values, got {}",
            array.value_length()
        ));
    }

    let source = array.values();
    let values = if source.as_ptr().align_offset(align_of::<T::Native>()) == 0 {
        ScalarBuffer::new(source.clone(), 0, array.len())
    } else {
        ScalarBuffer::new(Buffer::from(source.as_slice()), 0, array.len())
    };

    Ok(PrimitiveArray::<T>::new(values, array.nulls().cloned()))
}

/// Generic hash-set membership for the 4-, 8-, and 16-byte representations.
///
/// The standard primitive filters are concrete per Arrow type. This local
/// generic form lets the three supported widths share one implementation.
struct HashSetFilter<T: ArrowPrimitiveType> {
    null_count: usize,
    values: HashSet<T::Native>,
    _marker: PhantomData<T>,
}

impl<T> HashSetFilter<T>
where
    T: ArrowPrimitiveType,
    T::Native: Copy + Eq + Hash,
{
    fn new(in_array: &PrimitiveArray<T>) -> Self {
        let mut values = HashSet::with_capacity(in_array.len());
        for value in in_array.iter().flatten() {
            values.insert(value);
        }

        Self {
            null_count: in_array.null_count(),
            values,
            _marker: PhantomData,
        }
    }
}

impl<T> StaticFilter for HashSetFilter<T>
where
    T: ArrowPrimitiveType + Send + Sync + 'static,
    T::Native: Copy + Eq + Hash + Send + Sync,
{
    fn null_count(&self) -> usize {
        self.null_count
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        handle_dictionary!(self, v, negated);

        let v = v.as_primitive_opt::<T>().ok_or_else(|| {
            internal_datafusion_err!("HashSetFilter: expected {} array", T::DATA_TYPE)
        })?;
        let input_values = v.values();
        Ok(build_in_list_result(
            v.len(),
            v.nulls(),
            self.null_count > 0,
            negated,
            #[inline(always)]
            |index| self.values.contains(&input_values[index]),
        ))
    }
}

/// Adapts a primitive filter to concrete, same-width `FixedSizeBinary` arrays.
struct FixedSizeBinaryFilter<T: ArrowPrimitiveType> {
    data_type: DataType,
    inner: StaticFilterRef,
    _marker: PhantomData<T>,
}

impl<T: ArrowPrimitiveType> FixedSizeBinaryFilter<T> {
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
    T: ArrowPrimitiveType + Send + Sync + 'static,
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
        let primitive = as_primitive::<T>(array)?;
        self.inner.contains(&primitive, negated)
    }
}

fn branchless_or_bitmap<T>(
    array: &FixedSizeBinaryArray,
    count: usize,
) -> Result<StaticFilterRef>
where
    T: BranchlessFilterType + BitmapFilterType,
    BranchlessNative<T>: Copy + Eq + Send + Sync,
{
    let primitive: ArrayRef = Arc::new(as_primitive::<T>(array)?);
    let inner: StaticFilterRef = if use_branchless::<T>(count) {
        Arc::new(BranchlessFilter::<T>::try_new(&primitive)?)
    } else {
        Arc::new(BitmapFilter::<T>::try_new(&primitive)?)
    };
    Ok(Arc::new(FixedSizeBinaryFilter::<T>::new(
        array.data_type().clone(),
        inner,
    )))
}

fn branchless_or_hash_set<T>(
    array: &FixedSizeBinaryArray,
    count: usize,
) -> Result<StaticFilterRef>
where
    T: BranchlessFilterType,
    T::Native: Copy + Eq + Hash + Send + Sync,
    BranchlessNative<T>: Copy + Eq + Send + Sync,
{
    let primitive = as_primitive::<T>(array)?;
    let inner: StaticFilterRef = if use_branchless::<T>(count) {
        let primitive: ArrayRef = Arc::new(primitive);
        Arc::new(BranchlessFilter::<T>::try_new(&primitive)?)
    } else {
        Arc::new(HashSetFilter::<T>::new(&primitive))
    };
    Ok(Arc::new(FixedSizeBinaryFilter::<T>::new(
        array.data_type().clone(),
        inner,
    )))
}

/// Creates an optimized filter for supported concrete `FixedSizeBinary` arrays.
pub(super) fn instantiate_fixed_size_binary_filter(
    in_array: &ArrayRef,
) -> Result<Option<StaticFilterRef>> {
    let DataType::FixedSizeBinary(width) = in_array.data_type() else {
        return Ok(None);
    };
    let Some(array) = in_array.as_fixed_size_binary_opt() else {
        return Ok(None);
    };

    let count = array.len() - array.null_count();

    let filter = match width {
        1 => branchless_or_bitmap::<UInt8Type>(array, count)?,
        2 => branchless_or_bitmap::<UInt16Type>(array, count)?,
        4 => branchless_or_hash_set::<UInt32Type>(array, count)?,
        8 => branchless_or_hash_set::<UInt64Type>(array, count)?,
        16 => branchless_or_hash_set::<Decimal128Type>(array, count)?,
        _ => return Ok(None),
    };
    Ok(Some(filter))
}

#[cfg(test)]
mod tests {
    use arrow::array::{DictionaryArray, Int8Array, StringArray};
    use arrow::buffer::{Buffer, NullBuffer};
    use arrow::datatypes::Int8Type;

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
                Some(value(width, 7, false)),
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

    fn unaligned_array(
        width: i32,
        values: &[Vec<u8>],
        nulls: Option<NullBuffer>,
    ) -> FixedSizeBinaryArray {
        let mut bytes = vec![0];
        bytes.extend(values.iter().flatten());
        let buffer = Buffer::from(bytes).slice(1);
        assert_ne!(
            buffer.as_ptr().align_offset(width as usize),
            0,
            "test buffer must be unaligned"
        );
        FixedSizeBinaryArray::new(width, buffer, nulls)
    }

    #[test]
    fn handles_aligned_and_unaligned_buffers() -> Result<()> {
        let buffer = Buffer::from_vec(vec![1_u64, 2, 3]);
        let source_ptr = buffer.as_ptr();
        let array = FixedSizeBinaryArray::new(8, buffer, None);
        let primitive = as_primitive::<UInt64Type>(&array)?;
        assert_eq!(primitive.values().inner().as_ptr(), source_ptr);

        let width = 16;
        let haystack_values = (0..5)
            .map(|index| value(width, index, false))
            .collect::<Vec<_>>();
        let haystack: ArrayRef = Arc::new(unaligned_array(width, &haystack_values, None));
        let needles = unaligned_array(
            width,
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
