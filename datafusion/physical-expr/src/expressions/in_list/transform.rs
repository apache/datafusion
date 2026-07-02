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

//! Type transformation utilities for InList filters.
//!
//! Some filters only depend on fixed-width value bit patterns. For those cases,
//! compatible primitive arrays can be reinterpreted to the filter's unsigned
//! storage type without copying values.

use std::mem::size_of;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, PrimitiveArray};
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::{ArrowPrimitiveType, DataType};
use datafusion_common::{Result, exec_datafusion_err};

use super::primitive_filter::{BitmapFilter, BitmapFilterType};
use super::static_filter::{StaticFilter, handle_dictionary};

/// Bitmap filter for signed 1-byte and 2-byte primitive arrays.
///
/// The bitmap implementation is keyed by an unsigned primitive type (`UInt8` or
/// `UInt16`). This wrapper keeps the original array type, such as `Int8`, and
/// only reinterprets values as the unsigned type when probing the bitmap.
struct ReinterpretedBitmap<T: BitmapFilterType> {
    expected_data_type: DataType,
    inner: BitmapFilter<T>,
}

impl<T: BitmapFilterType> StaticFilter for ReinterpretedBitmap<T> {
    fn null_count(&self) -> usize {
        self.inner.null_count()
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        handle_dictionary!(self, v, negated);

        if v.data_type() != &self.expected_data_type {
            return Err(exec_datafusion_err!(
                "BitmapFilter: expected {} array, got {}",
                self.expected_data_type,
                v.data_type()
            ));
        }

        let data = v.to_data();
        let values: &[T::Native] = &data.buffer::<T::Native>(0)[..v.len()];

        Ok(self.inner.contains_slice(values, data.nulls(), negated))
    }
}

/// Views a primitive array as another primitive type with the same byte width.
///
/// This does not convert values. It reuses the existing values buffer and
/// interprets each value's bytes as `T::Native`, preserving the null buffer.
/// The caller must check that the source and target primitive types have the
/// same width.
#[inline]
pub(crate) fn reinterpret_any_primitive_to<T: ArrowPrimitiveType>(
    array: &dyn Array,
) -> ArrayRef {
    let data = array.to_data();
    let values = data.buffers()[0].clone();
    let buffer = ScalarBuffer::<T::Native>::new(values, data.offset(), data.len());
    Arc::new(PrimitiveArray::<T>::new(buffer, array.nulls().cloned()))
}

/// Creates a bitmap filter for 1-byte or 2-byte primitive arrays.
///
/// Unsigned inputs use the bitmap filter directly. Signed inputs of the same
/// width are reinterpreted as the unsigned bitmap type, without copying.
pub(crate) fn make_bitmap_filter<T>(
    in_array: &ArrayRef,
) -> Result<Arc<dyn StaticFilter + Send + Sync>>
where
    T: BitmapFilterType,
{
    if in_array.data_type() == &T::DATA_TYPE {
        return Ok(Arc::new(BitmapFilter::<T>::try_new(in_array)?));
    }

    let width = size_of::<T::Native>();
    if in_array.data_type().primitive_width() != Some(width) {
        return Err(exec_datafusion_err!(
            "BitmapFilter: expected {}-byte primitive array for {} bitmap, got {}",
            width,
            T::DATA_TYPE,
            in_array.data_type()
        ));
    }

    let reinterpreted = reinterpret_any_primitive_to::<T>(in_array.as_ref());
    let inner = BitmapFilter::<T>::try_new(&reinterpreted)?;
    Ok(Arc::new(ReinterpretedBitmap {
        expected_data_type: in_array.data_type().clone(),
        inner,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, BooleanArray, Int8Array, Int16Array, UInt8Array, UInt16Array,
    };
    use arrow::datatypes::{UInt8Type, UInt16Type};

    #[test]
    fn reinterpreted_bitmap_handles_signed_boundaries_and_slices() -> Result<()> {
        let haystack: ArrayRef = Arc::new(
            Int8Array::from(vec![Some(99), Some(i8::MIN), None, Some(-1), Some(42)])
                .slice(1, 3),
        );
        let filter = make_bitmap_filter::<UInt8Type>(&haystack)?;
        let needles =
            Int8Array::from(vec![Some(7), Some(i8::MIN), Some(-1), None]).slice(1, 3);

        assert_eq!(
            filter.contains(&needles, false)?,
            BooleanArray::from(vec![Some(true), Some(true), None])
        );
        assert_eq!(
            filter.contains(&needles, true)?,
            BooleanArray::from(vec![Some(false), Some(false), None])
        );

        let haystack: ArrayRef = Arc::new(
            Int16Array::from(vec![
                Some(123),
                Some(i16::MIN),
                None,
                Some(-1),
                Some(i16::MAX),
            ])
            .slice(1, 4),
        );
        let filter = make_bitmap_filter::<UInt16Type>(&haystack)?;
        let needles =
            Int16Array::from(vec![Some(0), Some(i16::MIN), Some(7), Some(i16::MAX)])
                .slice(1, 3);

        assert_eq!(
            filter.contains(&needles, false)?,
            BooleanArray::from(vec![Some(true), None, Some(true)])
        );
        assert_eq!(
            filter.contains(&needles, true)?,
            BooleanArray::from(vec![Some(false), None, Some(false)])
        );

        Ok(())
    }

    #[test]
    fn reinterpreted_bitmap_rejects_same_width_unsigned_needles() -> Result<()> {
        let haystack: ArrayRef = Arc::new(Int8Array::from(vec![Some(-1)]));
        let filter = make_bitmap_filter::<UInt8Type>(&haystack)?;
        let needles = UInt8Array::from(vec![Some(u8::MAX)]);
        let err = filter.contains(&needles, false).unwrap_err().to_string();
        assert!(err.contains("expected Int8 array, got UInt8"), "{err}");

        let haystack: ArrayRef = Arc::new(Int16Array::from(vec![Some(-1)]));
        let filter = make_bitmap_filter::<UInt16Type>(&haystack)?;
        let needles = UInt16Array::from(vec![Some(u16::MAX)]);
        let err = filter.contains(&needles, false).unwrap_err().to_string();
        assert!(err.contains("expected Int16 array, got UInt16"), "{err}");

        Ok(())
    }
}
