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
use super::static_filter::StaticFilter;
use super::transform::{make_bitmap_filter, make_branchless_filter};

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
                FilterStrategy::Generic
            }
        }
        Some(8) => {
            if len <= BRANCHLESS_MAX_8B {
                FilterStrategy::Branchless
            } else {
                FilterStrategy::Generic
            }
        }
        Some(16) => {
            if len <= BRANCHLESS_MAX_16B {
                FilterStrategy::Branchless
            } else {
                FilterStrategy::Generic
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

        // Fallback for larger primitive lists (Legacy HashSet) or complex types (NestedTypeFilter)
        (_, Generic) => match dt {
            DataType::Int32 => Ok(Arc::new(Int32StaticFilter::try_new(&in_array)?)),
            DataType::Int64 => Ok(Arc::new(Int64StaticFilter::try_new(&in_array)?)),
            DataType::UInt32 => Ok(Arc::new(UInt32StaticFilter::try_new(&in_array)?)),
            DataType::UInt64 => Ok(Arc::new(UInt64StaticFilter::try_new(&in_array)?)),
            DataType::Float32 => Ok(Arc::new(Float32StaticFilter::try_new(&in_array)?)),
            DataType::Float64 => Ok(Arc::new(Float64StaticFilter::try_new(&in_array)?)),
            _ => Ok(Arc::new(NestedTypeFilter::try_new(in_array)?)),
        },
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
