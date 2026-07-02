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

use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::compute::cast;
use arrow::datatypes::*;
use datafusion_common::{Result, exec_datafusion_err};

use super::array_static_filter::ArrayStaticFilter;
use super::primitive_filter::*;
use super::static_filter::{StaticFilter, handle_dictionary};
use super::transform::{
    make_bitmap_filter, make_branchless_filter, reinterpret_any_primitive_to,
};

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
    /// Direct-probe hash table for larger primitive lists.
    Hashed,
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

        // Hash filters for larger lists of primitives.
        (_, Hashed) => dispatch_hashed(&in_array).ok_or_else(|| {
            exec_datafusion_err!("Hashed strategy selected but no filter for {:?}", dt)
        })?,

        // Fallback for nested/complex types and strings.
        (_, Generic) => Ok(Arc::new(ArrayStaticFilter::try_new(in_array)?)),
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

fn dispatch_hashed(
    arr: &ArrayRef,
) -> Option<Result<Arc<dyn StaticFilter + Send + Sync>>> {
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

    // For other primitive types, reinterpret same-width bit patterns.
    match arr.data_type().primitive_width() {
        Some(4) => Some(make_direct_probe_filter_reinterpreted::<UInt32Type>(arr)),
        Some(8) => Some(make_direct_probe_filter_reinterpreted::<UInt64Type>(arr)),
        Some(16) => Some(make_direct_probe_filter_reinterpreted::<Decimal128Type>(
            arr,
        )),
        _ => None,
    }
}

/// Creates a DirectProbeFilter with type reinterpretation for same-width primitive types.
fn make_direct_probe_filter_reinterpreted<D>(
    in_array: &ArrayRef,
) -> Result<Arc<dyn StaticFilter + Send + Sync>>
where
    D: ArrowPrimitiveType + 'static,
    D::Native: Send + Sync + DirectProbeHashable + 'static,
{
    if in_array.data_type() == &D::DATA_TYPE {
        return Ok(Arc::new(DirectProbeFilter::<D>::try_new(in_array)?));
    }
    if in_array.data_type().primitive_width() != Some(size_of::<D::Native>()) {
        return Err(exec_datafusion_err!(
            "DirectProbeFilter: expected {}-byte primitive array, got {}",
            size_of::<D::Native>(),
            in_array.data_type()
        ));
    }

    let reinterpreted = reinterpret_any_primitive_to::<D>(in_array.as_ref());
    let inner = DirectProbeFilter::<D>::try_new(&reinterpreted)?;
    Ok(Arc::new(ReinterpretedDirectProbeFilter {
        expected_data_type: in_array.data_type().clone(),
        inner,
    }))
}

/// Wrapper for DirectProbeFilter with type reinterpretation.
struct ReinterpretedDirectProbeFilter<D: ArrowPrimitiveType>
where
    D::Native: DirectProbeHashable,
{
    expected_data_type: DataType,
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
    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        handle_dictionary!(self, v, negated);
        if v.data_type() != &self.expected_data_type {
            return Err(exec_datafusion_err!(
                "DirectProbeFilter: expected {} array, got {}",
                self.expected_data_type,
                v.data_type()
            ));
        }

        let data = v.to_data();
        let values: &[D::Native] = &data.buffer::<D::Native>(0)[..v.len()];
        Ok(self.inner.contains_slice(values, v.nulls(), negated))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{ArrayRef, BooleanArray, Float64Array};

    #[test]
    fn direct_probe_strategy_handles_reinterpreted_slices() -> Result<()> {
        let haystack: ArrayRef = Arc::new(
            Float64Array::from(vec![
                Some(999.0),
                Some(0.0),
                Some(1.0),
                None,
                Some(f64::NAN),
                Some(4.0),
                Some(5.0),
                Some(6.0),
                Some(7.0),
                Some(8.0),
                Some(9.0),
                Some(10.0),
                Some(11.0),
                Some(12.0),
                Some(13.0),
                Some(14.0),
                Some(15.0),
                Some(16.0),
                Some(17.0),
                Some(1234.0),
            ])
            .slice(1, 18),
        );
        let filter = instantiate_static_filter(haystack)?;
        let needles =
            Float64Array::from(vec![Some(999.0), Some(1.0), Some(3.0), Some(f64::NAN)])
                .slice(1, 3);

        assert_eq!(
            filter.contains(&needles, false)?,
            BooleanArray::from(vec![Some(true), None, Some(true)])
        );

        Ok(())
    }
}
