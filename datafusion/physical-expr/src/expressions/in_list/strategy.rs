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

use std::hash::Hash;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::*;
use datafusion_common::{Result, exec_datafusion_err};

use super::byte_filter::{ByteArrayAccess, ByteFilter}; // ByteFilter for Utf8/Binary
use super::nested_filter::NestedTypeFilter;
use super::primitive_filter::{PrimitiveFilter, U8Config, U16Config};
use super::static_filter::StaticFilter;
use super::transform::{
    make_bitmap_filter, make_branchless_filter, make_byte_view_masked_filter,
    make_primitive_filter, make_utf8view_branchless_filter, make_utf8view_hash_filter,
    utf8view_all_short_strings,
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

        // Byte array filters (Utf8, Binary variants)
        // Uses batch hashing + direct byte comparison
        (DataType::Utf8, Generic) => Ok(Arc::new(
            ByteFilter::<ByteArrayAccess<Utf8Type>>::try_new(in_array)?,
        )),
        (DataType::LargeUtf8, Generic) => Ok(Arc::new(ByteFilter::<
            ByteArrayAccess<LargeUtf8Type>,
        >::try_new(in_array)?)),
        (DataType::Binary, Generic) => Ok(Arc::new(ByteFilter::<
            ByteArrayAccess<BinaryType>,
        >::try_new(in_array)?)),
        (DataType::LargeBinary, Generic) => Ok(Arc::new(ByteFilter::<
            ByteArrayAccess<LargeBinaryType>,
        >::try_new(in_array)?)),

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

/// Dispatch macro that routes primitive types by width to the appropriate UInt type.
///
/// All primitive types (Int*, UInt*, Float*, Timestamp*, Date*, Duration*, etc.) are
/// automatically dispatched based on their width. The reinterpret function handles
/// the fast path when source type already matches the destination UInt type.
macro_rules! dispatch_primitive {
    ($arr:expr, $reinterpret:ident) => {
        match $arr.data_type().primitive_width() {
            Some(1) => Some($reinterpret::<UInt8Type>($arr)),
            Some(2) => Some($reinterpret::<UInt16Type>($arr)),
            Some(4) => Some($reinterpret::<UInt32Type>($arr)),
            Some(8) => Some($reinterpret::<UInt64Type>($arr)),
            Some(16) => Some($reinterpret::<Decimal128Type>($arr)),
            _ => None,
        }
    };
}

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
    // Fast path: create PrimitiveFilter directly for common hashable types
    macro_rules! direct_filter {
        ($T:ty) => {
            return Some(
                PrimitiveFilter::<$T>::try_new(arr)
                    .map(|f| Arc::new(f) as Arc<dyn StaticFilter + Send + Sync>),
            )
        };
    }
    match arr.data_type() {
        DataType::Int32 => direct_filter!(Int32Type),
        DataType::Int64 => direct_filter!(Int64Type),
        DataType::UInt32 => direct_filter!(UInt32Type),
        DataType::UInt64 => direct_filter!(UInt64Type),
        _ => {}
    }

    // For other types (Float32, Float64, Timestamp, etc.), reinterpret to UInt
    fn make<D: ArrowPrimitiveType + 'static>(
        arr: &ArrayRef,
    ) -> Result<Arc<dyn StaticFilter + Send + Sync>>
    where
        D::Native: Hash + Eq + Send + Sync + 'static,
    {
        make_primitive_filter::<D>(arr)
    }
    dispatch_primitive!(arr, make)
}
