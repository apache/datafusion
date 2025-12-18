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

//! Type transformation utilities for InList filters
//!
//! This module provides type reinterpretation for optimizing filter dispatch.
//! For equality comparison, only the bit pattern matters, so we can:
//! - Reinterpret signed integers as unsigned (Int32 → UInt32)
//! - Reinterpret floats as unsigned integers (Float64 → UInt64)
//!
//! This allows using a single filter implementation (e.g., for UInt64) to handle
//! multiple types (Int64, Float64, Timestamp, Duration) that share the same
//! byte width, reducing code duplication.

use std::hash::Hash;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, PrimitiveArray};
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::{ArrowPrimitiveType, Decimal128Type};
use datafusion_common::Result;

use super::array_filter::StaticFilter;
use super::primitive::{
    BitmapFilter, BitmapFilterConfig, BranchlessFilter, PrimitiveFilter,
};
use super::result::handle_dictionary;

/// Maximum length for inline strings in Utf8View (stored in 16-byte view).
pub(crate) const UTF8VIEW_INLINE_LEN: usize = 12;

// =============================================================================
// REINTERPRETING FILTERS (zero-copy type conversion)
// =============================================================================

/// Reinterpreting filter for hash-based lookups.
///
/// Zero-copy: reinterprets input buffer directly as target type slice.
struct ReinterpretedPrimitive<D: ArrowPrimitiveType> {
    inner: PrimitiveFilter<D>,
}

impl<D> StaticFilter for ReinterpretedPrimitive<D>
where
    D: ArrowPrimitiveType + 'static,
    D::Native: Hash + Eq + Send + Sync + 'static,
{
    fn null_count(&self) -> usize {
        self.inner.null_count()
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        handle_dictionary!(self, v, negated);

        let data = v.to_data();
        let values: &[D::Native] = data.buffers()[0].typed_data();

        Ok(self.inner.contains_slice(values, data.nulls(), negated))
    }
}

/// Reinterpreting filter for bitmap lookups (u8/u16).
struct ReinterpretedBitmap<C: BitmapFilterConfig> {
    inner: BitmapFilter<C>,
}

impl<C: BitmapFilterConfig> StaticFilter for ReinterpretedBitmap<C> {
    fn null_count(&self) -> usize {
        self.inner.null_count()
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        handle_dictionary!(self, v, negated);

        let data = v.to_data();
        let values: &[C::Native] = data.buffers()[0].typed_data();

        Ok(self.inner.contains_slice(values, data.nulls(), negated))
    }
}

/// Reinterpreting filter for branchless lookups.
struct ReinterpretedBranchless<T: ArrowPrimitiveType, const N: usize> {
    inner: BranchlessFilter<T, N>,
}

impl<T, const N: usize> StaticFilter for ReinterpretedBranchless<T, N>
where
    T: ArrowPrimitiveType + 'static,
    T::Native: Copy + PartialEq + Send + Sync + 'static,
{
    fn null_count(&self) -> usize {
        self.inner.null_count()
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        handle_dictionary!(self, v, negated);

        let data = v.to_data();
        let values: &[T::Native] = data.buffers()[0].typed_data();

        Ok(self.inner.contains_slice(values, data.nulls(), negated))
    }
}

/// Hash filter for Utf8View short strings (≤12 bytes).
///
/// Reinterprets the views buffer directly as i128 slice.
struct Utf8ViewHashFilter {
    inner: PrimitiveFilter<Decimal128Type>,
}

impl StaticFilter for Utf8ViewHashFilter {
    fn null_count(&self) -> usize {
        self.inner.null_count()
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        handle_dictionary!(self, v, negated);

        // Zero-copy: reinterpret views buffer directly as i128 slice
        let sv = v.as_string_view();
        let values: &[i128] = sv.views().inner().typed_data();

        Ok(self.inner.contains_slice(values, sv.nulls(), negated))
    }
}

/// Reinterprets any primitive-like array as the target primitive type T by extracting
/// the underlying buffer.
///
/// This is a zero-copy operation that works for all primitive types (Int*, UInt*, Float*,
/// Timestamp*, Date*, Duration*, etc.) by directly accessing the underlying buffer,
/// ignoring any metadata like timezones or precision/scale.
#[inline]
fn reinterpret_any_primitive_to<T: ArrowPrimitiveType>(array: &dyn Array) -> ArrayRef {
    let values = array.to_data().buffers()[0].clone();
    let buffer: ScalarBuffer<T::Native> = values.into();
    Arc::new(PrimitiveArray::<T>::new(buffer, array.nulls().cloned()))
}

/// Creates a hash-based filter, reinterpreting types if needed.
pub(crate) fn make_primitive_filter<D>(
    in_array: &ArrayRef,
) -> Result<Arc<dyn StaticFilter + Send + Sync>>
where
    D: ArrowPrimitiveType + 'static,
    D::Native: Hash + Eq + Send + Sync + 'static,
{
    if in_array.data_type() == &D::DATA_TYPE {
        return Ok(Arc::new(PrimitiveFilter::<D>::try_new(in_array)?));
    }

    let reinterpreted = reinterpret_any_primitive_to::<D>(in_array.as_ref());
    let inner = PrimitiveFilter::<D>::try_new(&reinterpreted)?;
    Ok(Arc::new(ReinterpretedPrimitive { inner }))
}

/// Creates a bitmap filter for u8/u16 types, reinterpreting if needed.
pub(crate) fn make_bitmap_filter<C>(
    in_array: &ArrayRef,
) -> Result<Arc<dyn StaticFilter + Send + Sync>>
where
    C: BitmapFilterConfig,
{
    if in_array.data_type() == &C::ArrowType::DATA_TYPE {
        return Ok(Arc::new(BitmapFilter::<C>::try_new(in_array)?));
    }

    let reinterpreted = reinterpret_any_primitive_to::<C::ArrowType>(in_array.as_ref());
    let inner = BitmapFilter::<C>::try_new(&reinterpreted)?;
    Ok(Arc::new(ReinterpretedBitmap { inner }))
}

/// Creates a branchless filter for small lists (≤16 elements), reinterpreting if needed.
pub(crate) fn make_branchless_filter<D>(
    in_array: &ArrayRef,
) -> Result<Arc<dyn StaticFilter + Send + Sync>>
where
    D: ArrowPrimitiveType + 'static,
    D::Native: Copy + PartialEq + Send + Sync + 'static,
{
    let reinterpreted = if in_array.data_type() == &D::DATA_TYPE {
        Arc::clone(in_array)
    } else {
        reinterpret_any_primitive_to::<D>(in_array.as_ref())
    };

    macro_rules! try_branchless {
        ($($n:literal),*) => {
            $(if let Some(Ok(inner)) = BranchlessFilter::<D, $n>::try_new(&reinterpreted) {
                if in_array.data_type() == &D::DATA_TYPE {
                    return Ok(Arc::new(inner));
                } else {
                    return Ok(Arc::new(ReinterpretedBranchless { inner }));
                }
            })*
        };
    }
    try_branchless!(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16);

    datafusion_common::exec_err!(
        "Branchless filter only supports 0-16 elements, got {}",
        in_array.len() - in_array.null_count()
    )
}

// =============================================================================
// UTF8VIEW REINTERPRETATION (short strings ≤12 bytes → Decimal128)
// =============================================================================

/// Checks if all strings in a Utf8View array are short enough to be inline.
///
/// In Utf8View, strings ≤12 bytes are stored inline in the 16-byte view struct.
/// These can be reinterpreted as i128 for fast equality comparison.
#[inline]
pub(crate) fn utf8view_all_short_strings(array: &dyn Array) -> bool {
    let sv = array.as_string_view();
    sv.views().iter().enumerate().all(|(i, &view)| {
        !sv.is_valid(i) || (view as u32) as usize <= UTF8VIEW_INLINE_LEN
    })
}

/// Reinterprets a Utf8View array as Decimal128 by treating the view bytes as i128.
#[inline]
fn reinterpret_utf8view_as_decimal128(array: &dyn Array) -> ArrayRef {
    let sv = array.as_string_view();
    let buffer: ScalarBuffer<i128> = sv.views().inner().clone().into();
    Arc::new(PrimitiveArray::<Decimal128Type>::new(
        buffer,
        sv.nulls().cloned(),
    ))
}

/// Creates a hash filter for Utf8View arrays with short strings.
pub(crate) fn make_utf8view_hash_filter(
    in_array: &ArrayRef,
) -> Result<Arc<dyn StaticFilter + Send + Sync>> {
    let reinterpreted = reinterpret_utf8view_as_decimal128(in_array.as_ref());
    let inner = PrimitiveFilter::<Decimal128Type>::try_new(&reinterpreted)?;
    Ok(Arc::new(Utf8ViewHashFilter { inner }))
}

/// Creates a branchless filter for Utf8View arrays with short strings.
pub(crate) fn make_utf8view_branchless_filter(
    in_array: &ArrayRef,
) -> Result<Arc<dyn StaticFilter + Send + Sync>> {
    let reinterpreted = reinterpret_utf8view_as_decimal128(in_array.as_ref());

    macro_rules! try_branchless {
        ($($n:literal),*) => {
            $(if let Some(Ok(inner)) = BranchlessFilter::<Decimal128Type, $n>::try_new(&reinterpreted) {
                return Ok(Arc::new(Utf8ViewBranchless { inner }));
            })*
        };
    }
    try_branchless!(0, 1, 2, 3, 4);

    datafusion_common::exec_err!(
        "Utf8View branchless filter only supports 0-4 elements, got {}",
        in_array.len() - in_array.null_count()
    )
}

/// Branchless filter for Utf8View short strings (≤12 bytes).
struct Utf8ViewBranchless<const N: usize> {
    inner: BranchlessFilter<Decimal128Type, N>,
}

impl<const N: usize> StaticFilter for Utf8ViewBranchless<N> {
    fn null_count(&self) -> usize {
        self.inner.null_count()
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        handle_dictionary!(self, v, negated);

        let sv = v.as_string_view();
        let values: &[i128] = sv.views().inner().typed_data();

        Ok(self.inner.contains_slice(values, sv.nulls(), negated))
    }
}
