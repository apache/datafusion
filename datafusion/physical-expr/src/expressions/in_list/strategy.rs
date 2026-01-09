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

use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::compute::cast;
use arrow::datatypes::*;
use datafusion_common::{Result, exec_datafusion_err};

use super::array_static_filter::ArrayStaticFilter;
use super::primitive_filter::*;
use super::static_filter::StaticFilter;
use super::transform::{make_bitmap_filter, make_branchless_filter};

/// Maximum list size for branchless lookup on 1-byte primitives (Int8, UInt8).
const BRANCHLESS_MAX_1B: usize = 16;

/// Maximum list size for branchless lookup on 2-byte primitives (Int16, UInt16).
const BRANCHLESS_MAX_2B: usize = 8;

/// Maximum list size for branchless lookup on 4-byte primitives (Int32, UInt32, Float32).
const BRANCHLESS_MAX_4B: usize = 32;

/// Maximum list size for branchless lookup on 8-byte primitives (Int64, UInt64, Float64).
const BRANCHLESS_MAX_8B: usize = 16;

/// Maximum list size for branchless lookup on 16-byte types (Decimal128).
const BRANCHLESS_MAX_16B: usize = 4;

/// The lookup strategy to use for a given data type and list size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilterStrategy {
    /// Bitmap filter for u8/u16 domains.
    Bitmap1B,
    Bitmap2B,
    /// Branchless OR-chain for small lists.
    Branchless,
    /// Generic ArrayStaticFilter fallback.
    Generic,
}

/// Selects the lookup strategy based on data type and list size.
fn select_strategy(dt: &DataType, len: usize) -> FilterStrategy {
    match dt.primitive_width() {
        Some(1) => {
            if len <= BRANCHLESS_MAX_1B {
                FilterStrategy::Branchless
            } else {
                FilterStrategy::Bitmap1B
            }
        }
        Some(2) => {
            if len <= BRANCHLESS_MAX_2B {
                FilterStrategy::Branchless
            } else {
                FilterStrategy::Bitmap2B
            }
        }
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

/// Creates the optimal static filter for the given array.
pub(super) fn instantiate_static_filter(
    in_array: ArrayRef,
) -> Result<Arc<dyn StaticFilter + Send + Sync>> {
    // Flatten dictionary-encoded haystacks to their value type so that
    // specialized filters (e.g. Int32StaticFilter) are used instead of
    // falling through to the generic ArrayStaticFilter.
    let in_array = match in_array.data_type() {
        DataType::Dictionary(_, value_type) => cast(&in_array, value_type.as_ref())?,
        _ => in_array,
    };
    use FilterStrategy::*;

    let len = in_array.len();
    let dt = in_array.data_type();
    let strategy = select_strategy(dt, len);

    match (dt, strategy) {
        // Bitmap filters for 1-byte and 2-byte types
        (_, Bitmap1B) => make_bitmap_filter::<UInt8Type>(&in_array),
        (_, Bitmap2B) => make_bitmap_filter::<UInt16Type>(&in_array),

        // Branchless filters for small lists of primitives
        (_, Branchless) => dispatch_branchless(&in_array).ok_or_else(|| {
            exec_datafusion_err!(
                "Branchless strategy selected but no filter for {:?}",
                dt
            )
        })?,

        // Fallback for larger primitive lists or complex types.
        (_, Generic) => match dt {
            DataType::Int32 => Ok(Arc::new(Int32StaticFilter::try_new(&in_array)?)),
            DataType::Int64 => Ok(Arc::new(Int64StaticFilter::try_new(&in_array)?)),
            DataType::UInt32 => Ok(Arc::new(UInt32StaticFilter::try_new(&in_array)?)),
            DataType::UInt64 => Ok(Arc::new(UInt64StaticFilter::try_new(&in_array)?)),
            DataType::Float32 => Ok(Arc::new(Float32StaticFilter::try_new(&in_array)?)),
            DataType::Float64 => Ok(Arc::new(Float64StaticFilter::try_new(&in_array)?)),
            _ => Ok(Arc::new(ArrayStaticFilter::try_new(in_array)?)),
        },
    }
}

fn dispatch_branchless(
    arr: &ArrayRef,
) -> Option<Result<Arc<dyn StaticFilter + Send + Sync>>> {
    // Dispatch to width-specific branchless filter.
    match arr.data_type().primitive_width() {
        Some(1) => Some(make_branchless_filter::<UInt8Type>(arr)),
        Some(2) => Some(make_branchless_filter::<UInt16Type>(arr)),
        Some(4) => Some(make_branchless_filter::<UInt32Type>(arr)),
        Some(8) => Some(make_branchless_filter::<UInt64Type>(arr)),
        Some(16) => Some(make_branchless_filter::<Decimal128Type>(arr)),
        _ => None,
    }
}
