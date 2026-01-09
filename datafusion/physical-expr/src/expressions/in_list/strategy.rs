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
    make_utf8view_branchless_filter, make_utf8view_hash_filter,
    reinterpret_any_primitive_to, utf8view_all_short_strings,
};

// =============================================================================
// LOOKUP STRATEGY THRESHOLDS (tuned via microbenchmarks)
// =============================================================================

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

        // Byte view filters (Utf8View, BinaryView)
        (DataType::Utf8View, Generic) => {
            make_byte_view_masked_filter::<StringViewType>(in_array)
        }
        (DataType::BinaryView, Generic) => {
            make_byte_view_masked_filter::<BinaryViewType>(in_array)
        }

        // Fallback for nested/complex types and strings (Phase 4: Strings use fallback)
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
