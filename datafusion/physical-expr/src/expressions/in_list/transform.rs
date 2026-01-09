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

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, PrimitiveArray};
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::ArrowPrimitiveType;
use datafusion_common::Result;

use super::primitive_filter::{BitmapFilter, BitmapFilterConfig, BranchlessFilter};
use super::result::handle_dictionary;
use super::static_filter::StaticFilter;

// =============================================================================
// REINTERPRETING FILTERS (zero-copy type conversion)
// =============================================================================

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
        let values: &[C::Native] = data.buffer::<C::Native>(0);

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
        let values: &[T::Native] = data.buffer::<T::Native>(0);

        Ok(self.inner.contains_slice(values, data.nulls(), negated))
    }
}

/// Reinterprets any primitive-like array as the target primitive type T by extracting
/// the underlying buffer.
///
/// This is a zero-copy operation that works for all primitive types (Int*, UInt*, Float*,
/// Timestamp*, Date*, Duration*, etc.) by directly accessing the underlying buffer,
/// ignoring any metadata like timezones or precision/scale.
#[inline]
pub(crate) fn reinterpret_any_primitive_to<T: ArrowPrimitiveType>(
    array: &dyn Array,
) -> ArrayRef {
    let values = array.to_data().buffers()[0].clone();
    let buffer: ScalarBuffer<T::Native> = values.into();
    Arc::new(PrimitiveArray::<T>::new(buffer, array.nulls().cloned()))
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

// =============================================================================
// BRANCHLESS FILTER CREATION (const generic dispatch)
// =============================================================================

/// Creates a branchless filter for primitive types.
///
/// Dispatches based on byte width and element count:
/// - 4-byte types (Int32, Float32, etc.): supports 0-32 elements
/// - 8-byte types (Int64, Float64, Timestamp, etc.): supports 0-16 elements
/// - 16-byte types (Decimal128): supports 0-4 elements
pub(crate) fn make_branchless_filter<D>(
    in_array: &ArrayRef,
    width: usize,
) -> Result<Arc<dyn StaticFilter + Send + Sync>>
where
    D: ArrowPrimitiveType + 'static,
    D::Native: Copy + PartialEq + Send + Sync + 'static,
{
    let is_native = in_array.data_type() == &D::DATA_TYPE;
    let arr = if is_native {
        Arc::clone(in_array)
    } else {
        reinterpret_any_primitive_to::<D>(in_array.as_ref())
    };
    let n = arr.len() - arr.null_count();

    // Helper to create the filter for a known size N
    #[inline]
    fn create<D: ArrowPrimitiveType + 'static, const N: usize>(
        arr: &ArrayRef,
        is_native: bool,
    ) -> Result<Arc<dyn StaticFilter + Send + Sync>>
    where
        D::Native: Copy + PartialEq + Send + Sync + 'static,
    {
        let inner = BranchlessFilter::<D, N>::try_new(arr)
            .expect("size verified")
            .expect("type verified");
        if is_native {
            Ok(Arc::new(inner))
        } else {
            Ok(Arc::new(ReinterpretedBranchless { inner }))
        }
    }

    // Match on (width, count) - shared sizes use or-patterns to avoid duplication
    match (width, n) {
        // All widths: 0-4
        (4 | 8 | 16, 0) => create::<D, 0>(&arr, is_native),
        (4 | 8 | 16, 1) => create::<D, 1>(&arr, is_native),
        (4 | 8 | 16, 2) => create::<D, 2>(&arr, is_native),
        (4 | 8 | 16, 3) => create::<D, 3>(&arr, is_native),
        (4 | 8 | 16, 4) => create::<D, 4>(&arr, is_native),
        // 4-byte and 8-byte: 5-16
        (4 | 8, 5) => create::<D, 5>(&arr, is_native),
        (4 | 8, 6) => create::<D, 6>(&arr, is_native),
        (4 | 8, 7) => create::<D, 7>(&arr, is_native),
        (4 | 8, 8) => create::<D, 8>(&arr, is_native),
        (4 | 8, 9) => create::<D, 9>(&arr, is_native),
        (4 | 8, 10) => create::<D, 10>(&arr, is_native),
        (4 | 8, 11) => create::<D, 11>(&arr, is_native),
        (4 | 8, 12) => create::<D, 12>(&arr, is_native),
        (4 | 8, 13) => create::<D, 13>(&arr, is_native),
        (4 | 8, 14) => create::<D, 14>(&arr, is_native),
        (4 | 8, 15) => create::<D, 15>(&arr, is_native),
        (4 | 8, 16) => create::<D, 16>(&arr, is_native),
        // 4-byte only: 17-32
        (4, 17) => create::<D, 17>(&arr, is_native),
        (4, 18) => create::<D, 18>(&arr, is_native),
        (4, 19) => create::<D, 19>(&arr, is_native),
        (4, 20) => create::<D, 20>(&arr, is_native),
        (4, 21) => create::<D, 21>(&arr, is_native),
        (4, 22) => create::<D, 22>(&arr, is_native),
        (4, 23) => create::<D, 23>(&arr, is_native),
        (4, 24) => create::<D, 24>(&arr, is_native),
        (4, 25) => create::<D, 25>(&arr, is_native),
        (4, 26) => create::<D, 26>(&arr, is_native),
        (4, 27) => create::<D, 27>(&arr, is_native),
        (4, 28) => create::<D, 28>(&arr, is_native),
        (4, 29) => create::<D, 29>(&arr, is_native),
        (4, 30) => create::<D, 30>(&arr, is_native),
        (4, 31) => create::<D, 31>(&arr, is_native),
        (4, 32) => create::<D, 32>(&arr, is_native),
        // Error cases
        (4, n) => datafusion_common::exec_err!(
            "Branchless filter for 4-byte types supports 0-32 elements, got {n}"
        ),
        (8, n) => datafusion_common::exec_err!(
            "Branchless filter for 8-byte types supports 0-16 elements, got {n}"
        ),
        (16, n) => datafusion_common::exec_err!(
            "Branchless filter for 16-byte types supports 0-4 elements, got {n}"
        ),
        (w, _) => datafusion_common::exec_err!(
            "Branchless filter not supported for {w}-byte types"
        ),
    }
}
