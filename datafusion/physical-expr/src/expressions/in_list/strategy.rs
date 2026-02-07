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

//! Filter selection strategy for InList expressions
//!
//! Selects the optimal lookup strategy based on data type and list size:
//!
//! - 1-byte types (Int8/UInt8): bitmap (32 bytes, O(1) bit test)
//! - 2-byte types (Int16/UInt16): bitmap (8 KB, O(1) bit test)
//! - 4-byte types (Int32/Float32): branchless (≤32) or hash (>32)
//! - 8-byte types (Int64/Float64): branchless (≤16) or hash (>16)
//! - 16-byte types (Decimal128): branchless (≤4) or hash (>4)
//! - Utf8View (short strings): branchless (≤4) or hash (>4)
//! - Byte arrays (Utf8, Binary, etc.): ByteArrayFilter / ByteViewFilter
//! - Other types: NestedTypeFilter (fallback for List, Struct, Map, etc.)

use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::*;
use datafusion_common::{Result, exec_datafusion_err};

use super::nested_filter::NestedTypeFilter;
use super::primitive_filter::*;
use super::result::handle_dictionary;
use super::static_filter::StaticFilter;
use super::transform::{
    make_bitmap_filter, make_branchless_filter, make_byte_view_masked_filter,
    make_utf8_two_stage_filter, make_utf8view_branchless_filter,
    make_utf8view_hash_filter, utf8_all_short_strings, utf8view_all_short_strings,
};

// =============================================================================
// LOOKUP STRATEGY THRESHOLDS (tuned via microbenchmarks)
// =============================================================================
//
// Based on minimum batch time (8192 lookups per batch):
// - Int8 (1 byte): BITMAP (32 bytes, always fastest)
// - Int16 (2 bytes): BITMAP (8 KB, always fastest)
// - Int32 (4 bytes): branchless up to 32, then hashset
// - Int64 (8 bytes): branchless up to 16, then hashset
// - Int128 (16 bytes): branchless up to 4, then hashset
// - Byte arrays: ByteArrayFilter / ByteViewFilter
// - Other types: NestedTypeFilter (fallback for List, Struct, Map, etc.)
//
// NOTE: Binary search and linear scan were benchmarked but consistently
// lost to the strategies above at all tested list sizes.

/// Maximum list size for branchless lookup on 4-byte primitives (Int32, UInt32, Float32).
const BRANCHLESS_MAX_4B: usize = 32;

/// Maximum list size for branchless lookup on 8-byte primitives (Int64, UInt64, Float64).
const BRANCHLESS_MAX_8B: usize = 16;

/// Maximum list size for branchless lookup on 16-byte types (Decimal128).
const BRANCHLESS_MAX_16B: usize = 4;

// =============================================================================
// FILTER STRATEGY SELECTION
// =============================================================================

/// The lookup strategy to use for a given data type and list size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilterStrategy {
    /// Bitmap filter for u8/u16 - O(1) bit test, always fastest for these types.
    Bitmap1B,
    Bitmap2B,
    /// Branchless OR-chain for small lists.
    Branchless,
    /// HashSet for larger lists.
    Hashed,
    /// Generic ArrayStaticFilter fallback.
    Generic,
}

/// Determines the optimal lookup strategy based on data type and list size.
///
/// For 1-byte and 2-byte types, bitmap is always used (benchmarks show it's
/// faster than both branchless and hashed at all list sizes).
/// For larger types, cutoffs are tuned per byte-width.
fn select_strategy(dt: &DataType, len: usize) -> FilterStrategy {
    match dt.primitive_width() {
        Some(1) => FilterStrategy::Bitmap1B,
        Some(2) => FilterStrategy::Bitmap2B,
        Some(4) => {
            if len <= BRANCHLESS_MAX_4B {
                FilterStrategy::Branchless
            } else {
                FilterStrategy::Hashed
            }
        }
        Some(8) => {
            if len <= BRANCHLESS_MAX_8B {
                FilterStrategy::Branchless
            } else {
                FilterStrategy::Hashed
            }
        }
        Some(16) => {
            if len <= BRANCHLESS_MAX_16B {
                FilterStrategy::Branchless
            } else {
                FilterStrategy::Hashed
            }
        }
        _ => FilterStrategy::Generic,
    }
}

// =============================================================================
// FILTER INSTANTIATION
// =============================================================================

/// Creates the optimal static filter for the given array.
///
/// This is the main entry point for filter creation. It analyzes the array's
/// data type and size to select the best lookup strategy.
pub(crate) fn instantiate_static_filter(
    in_array: ArrayRef,
) -> Result<Arc<dyn StaticFilter + Send + Sync>> {
    use FilterStrategy::*;

    let len = in_array.len();
    let dt = in_array.data_type();

    // Special case: Utf8View with short strings can be reinterpreted as i128
    if matches!(dt, DataType::Utf8View) && utf8view_all_short_strings(in_array.as_ref()) {
        return if len <= BRANCHLESS_MAX_16B {
            make_utf8view_branchless_filter(&in_array)
        } else {
            make_utf8view_hash_filter(&in_array)
        };
    }

    let strategy = select_strategy(dt, len);

    match (dt, strategy) {
        // Bitmap filters for 1-byte and 2-byte types
        (_, Bitmap1B) => make_bitmap_filter::<U8Config>(&in_array),
        (_, Bitmap2B) => make_bitmap_filter::<U16Config>(&in_array),

        // Branchless filters for small lists of primitives
        (_, Branchless) => dispatch_branchless(&in_array).ok_or_else(|| {
            exec_datafusion_err!(
                "Branchless strategy selected but no filter for {:?}",
                dt
            )
        })?,

        // Hash filters for larger lists of primitives
        (_, Hashed) => dispatch_hashed(&in_array).ok_or_else(|| {
            exec_datafusion_err!("Hashed strategy selected but no filter for {:?}", dt)
        })?,

        // Utf8/LargeUtf8: Two-stage filter when all IN-list strings are short (≤12 bytes).
        // Stage 1 encodes as i128 (length + first 12 bytes) for O(1) rejection.
        // When strings are long, the encoding can't definitively match and the
        // overhead regresses vs the generic fallback, so we skip it.
        (DataType::Utf8 | DataType::LargeUtf8, Generic)
            if utf8_all_short_strings(in_array.as_ref()) =>
        {
            make_utf8_two_stage_filter(in_array)
        }

        // Binary variants: Use NestedTypeFilter (make_comparator)
        (DataType::Binary | DataType::LargeBinary, Generic) => {
            Ok(Arc::new(NestedTypeFilter::try_new(in_array)?))
        }

        // Byte view filters (Utf8View, BinaryView)
        // Both use two-stage filter: masked view pre-check + full verification
        (DataType::Utf8View, Generic) => {
            make_byte_view_masked_filter::<StringViewType>(in_array)
        }
        (DataType::BinaryView, Generic) => {
            make_byte_view_masked_filter::<BinaryViewType>(in_array)
        }

        // Fallback for nested/complex types (List, Struct, Map, Union, etc.)
        (_, Generic) => Ok(Arc::new(NestedTypeFilter::try_new(in_array)?)),
    }
}

// =============================================================================
// TYPE DISPATCH
// =============================================================================

fn dispatch_branchless(
    arr: &ArrayRef,
) -> Option<Result<Arc<dyn StaticFilter + Send + Sync>>> {
    // Dispatch to width-specific branchless filter.
    // Each width has its own max size: 4B→32, 8B→16, 16B→4
    match arr.data_type().primitive_width() {
        Some(4) => Some(make_branchless_filter::<UInt32Type>(arr, 4)),
        Some(8) => Some(make_branchless_filter::<UInt64Type>(arr, 8)),
        Some(16) => Some(make_branchless_filter::<Decimal128Type>(arr, 16)),
        _ => None,
    }
}

fn dispatch_hashed(
    arr: &ArrayRef,
) -> Option<Result<Arc<dyn StaticFilter + Send + Sync>>> {
    // Use DirectProbeFilter for fast hash table lookups
    macro_rules! direct_probe_filter {
        ($T:ty) => {
            return Some(
                DirectProbeFilter::<$T>::try_new(arr)
                    .map(|f| Arc::new(f) as Arc<dyn StaticFilter + Send + Sync>),
            )
        };
    }
    match arr.data_type() {
        DataType::Int32 => direct_probe_filter!(Int32Type),
        DataType::Int64 => direct_probe_filter!(Int64Type),
        DataType::UInt32 => direct_probe_filter!(UInt32Type),
        DataType::UInt64 => direct_probe_filter!(UInt64Type),
        _ => {}
    }

    // For other primitive types, reinterpret bits as appropriate UInt/Int type
    match arr.data_type().primitive_width() {
        Some(4) => Some(make_direct_probe_filter_reinterpreted::<UInt32Type>(arr)),
        Some(8) => Some(make_direct_probe_filter_reinterpreted::<UInt64Type>(arr)),
        Some(16) => Some(make_direct_probe_filter_reinterpreted::<Decimal128Type>(
            arr,
        )),
        // Other widths (1, 2) use Bitmap strategy and never reach here.
        // Unknown widths fall through to Generic strategy.
        _ => None,
    }
}

/// Creates a DirectProbeFilter with type reinterpretation for Float types
fn make_direct_probe_filter_reinterpreted<D>(
    in_array: &ArrayRef,
) -> Result<Arc<dyn StaticFilter + Send + Sync>>
where
    D: ArrowPrimitiveType + 'static,
    D::Native: Send + Sync + DirectProbeHashable + 'static,
{
    use super::transform::reinterpret_any_primitive_to;

    // Fast path: already the right type
    if in_array.data_type() == &D::DATA_TYPE {
        return Ok(Arc::new(DirectProbeFilter::<D>::try_new(in_array)?));
    }

    // Reinterpret and create filter
    let reinterpreted = reinterpret_any_primitive_to::<D>(in_array.as_ref());
    let inner = DirectProbeFilter::<D>::try_new(&reinterpreted)?;
    Ok(Arc::new(ReinterpretedDirectProbeFilter { inner }))
}

/// Wrapper for DirectProbeFilter with type reinterpretation
struct ReinterpretedDirectProbeFilter<D: ArrowPrimitiveType>
where
    D::Native: DirectProbeHashable,
{
    inner: DirectProbeFilter<D>,
}

impl<D> StaticFilter for ReinterpretedDirectProbeFilter<D>
where
    D: ArrowPrimitiveType + 'static,
    D::Native: Send + Sync + DirectProbeHashable + 'static,
{
    #[inline]
    fn null_count(&self) -> usize {
        self.inner.null_count()
    }

    #[inline]
    fn contains(
        &self,
        v: &dyn arrow::array::Array,
        negated: bool,
    ) -> Result<arrow::array::BooleanArray> {
        handle_dictionary!(self, v, negated);
        // Reinterpret needle array to destination type and use inner filter's raw slice path
        let data = v.to_data();
        let values: &[D::Native] = data.buffer::<D::Native>(0);
        Ok(self.inner.contains_slice(values, v.nulls(), negated))
    }
}
