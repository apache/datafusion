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

use super::primitive_filter::{BitmapFilter, BitmapFilterConfig};
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
