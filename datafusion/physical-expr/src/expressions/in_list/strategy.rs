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

use std::{hash::Hash, sync::Arc};

use arrow::array::ArrayRef;
use arrow::compute::cast;
use arrow::datatypes::{
    DataType, Date32Type, Date64Type, Decimal128Type, DurationMicrosecondType,
    DurationMillisecondType, DurationNanosecondType, DurationSecondType, Float16Type,
    Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
    IntervalMonthDayNanoType, IntervalUnit, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type,
};
use datafusion_common::Result;

use super::array_static_filter::ArrayStaticFilter;
use super::branchless_filter::{
    BranchlessFilter, BranchlessFilterType, BranchlessNative,
};
use super::byte_view_filter::instantiate_byte_view_filter;
use super::primitive_filter::*;
use super::static_filter::StaticFilter;

type StaticFilterRef = Arc<dyn StaticFilter + Send + Sync>;

pub(super) fn instantiate_static_filter(
    in_array: ArrayRef,
    expr_data_type: &DataType,
) -> Result<StaticFilterRef> {
    let in_array = flatten_dictionary_haystack(in_array)?;

    // Byte-view filters inspect the physical view representation directly.
    if dictionary_value_type(expr_data_type) == in_array.data_type()
        && let Some(filter) = instantiate_byte_view_filter(&in_array)?
    {
        return Ok(filter);
    }

    if let Some(filter) = instantiate_primitive_filter(&in_array)? {
        return Ok(filter);
    }

    Ok(Arc::new(ArrayStaticFilter::try_new(in_array)?))
}

fn dictionary_value_type(mut data_type: &DataType) -> &DataType {
    while let DataType::Dictionary(_, value_type) = data_type {
        data_type = value_type;
    }
    data_type
}

fn flatten_dictionary_haystack(in_array: ArrayRef) -> Result<ArrayRef> {
    // Flatten dictionary-encoded haystacks to their value type so that
    // specialized primitive filters are used instead of falling through to
    // the generic ArrayStaticFilter.
    match in_array.data_type() {
        DataType::Dictionary(_, value_type) => Ok(cast(&in_array, value_type.as_ref())?),
        _ => Ok(in_array),
    }
}

fn instantiate_primitive_filter(in_array: &ArrayRef) -> Result<Option<StaticFilterRef>> {
    let non_null_count = in_array.len() - in_array.null_count();

    macro_rules! filter {
        ($arrow_type:ty, $strategy:ident) => {
            $strategy::<$arrow_type>(in_array, non_null_count)
        };
    }

    match in_array.data_type() {
        DataType::Int8 => filter!(Int8Type, branchless_or_bitmap_filter),
        DataType::UInt8 => filter!(UInt8Type, branchless_or_bitmap_filter),
        DataType::Int16 => filter!(Int16Type, branchless_or_bitmap_filter),
        DataType::UInt16 => filter!(UInt16Type, branchless_or_bitmap_filter),
        DataType::Float16 => filter!(Float16Type, branchless_or_bitmap_filter),
        DataType::Int32 => filter!(Int32Type, branchless_or_frozen_filter),
        DataType::UInt32 => filter!(UInt32Type, branchless_or_frozen_filter),
        DataType::Float32 => filter!(Float32Type, branchless_or_frozen_filter),
        DataType::Date32 => filter!(Date32Type, branchless_or_frozen_filter),
        DataType::Time32(unit) => match unit {
            TimeUnit::Second => {
                filter!(Time32SecondType, branchless_or_frozen_filter)
            }
            TimeUnit::Millisecond => {
                filter!(Time32MillisecondType, branchless_or_frozen_filter)
            }
            _ => Ok(None),
        },
        DataType::Int64 => filter!(Int64Type, branchless_or_frozen_filter),
        DataType::UInt64 => filter!(UInt64Type, branchless_or_frozen_filter),
        DataType::Float64 => filter!(Float64Type, branchless_or_frozen_filter),
        DataType::Date64 => filter!(Date64Type, branchless_or_frozen_filter),
        DataType::Time64(unit) => match unit {
            TimeUnit::Microsecond => {
                filter!(Time64MicrosecondType, branchless_or_frozen_filter)
            }
            TimeUnit::Nanosecond => {
                filter!(Time64NanosecondType, branchless_or_frozen_filter)
            }
            _ => Ok(None),
        },
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => {
                filter!(TimestampSecondType, branchless_or_frozen_filter)
            }
            TimeUnit::Millisecond => {
                filter!(TimestampMillisecondType, branchless_or_frozen_filter)
            }
            TimeUnit::Microsecond => {
                filter!(TimestampMicrosecondType, branchless_or_frozen_filter)
            }
            TimeUnit::Nanosecond => {
                filter!(TimestampNanosecondType, branchless_or_frozen_filter)
            }
        },
        DataType::Duration(unit) => match unit {
            TimeUnit::Second => {
                filter!(DurationSecondType, branchless_or_frozen_filter)
            }
            TimeUnit::Millisecond => {
                filter!(DurationMillisecondType, branchless_or_frozen_filter)
            }
            TimeUnit::Microsecond => {
                filter!(DurationMicrosecondType, branchless_or_frozen_filter)
            }
            TimeUnit::Nanosecond => {
                filter!(DurationNanosecondType, branchless_or_frozen_filter)
            }
        },
        DataType::Decimal128(_, _) => {
            filter!(Decimal128Type, branchless_filter)
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            filter!(IntervalMonthDayNanoType, branchless_filter)
        }
        _ => Ok(None),
    }
}

fn branchless_or_bitmap_filter<T>(
    in_array: &ArrayRef,
    non_null_count: usize,
) -> Result<Option<StaticFilterRef>>
where
    T: BranchlessFilterType + BitmapFilterType,
    BranchlessNative<T>: Copy + PartialEq + Send + Sync,
{
    if let Some(filter) = branchless_filter::<T>(in_array, non_null_count)? {
        return Ok(Some(filter));
    }

    Ok(Some(Arc::new(BitmapFilter::<T>::try_new(in_array)?)))
}

fn branchless_or_frozen_filter<T>(
    in_array: &ArrayRef,
    non_null_count: usize,
) -> Result<Option<StaticFilterRef>>
where
    T: BranchlessFilterType,
    BranchlessNative<T>: Copy + Eq + Hash + Send + Sync,
{
    if let Some(filter) = branchless_filter::<T>(in_array, non_null_count)? {
        return Ok(Some(filter));
    }

    primitive_frozen_filter::<T>(in_array, non_null_count)
}

fn primitive_frozen_filter<T>(
    in_array: &ArrayRef,
    non_null_count: usize,
) -> Result<Option<StaticFilterRef>>
where
    T: BranchlessFilterType,
    BranchlessNative<T>: Copy + Eq + Hash + Send + Sync,
{
    if non_null_count <= T::MAX_LIST_LEN {
        return Ok(None);
    }

    Ok(Some(Arc::new(PrimitiveFrozenFilter::<T>::try_new(
        in_array,
    )?)))
}

fn branchless_filter<T>(
    in_array: &ArrayRef,
    non_null_count: usize,
) -> Result<Option<StaticFilterRef>>
where
    T: BranchlessFilterType,
    BranchlessNative<T>: Copy + PartialEq + Send + Sync,
{
    // Larger lists use another filter strategy. `try_new` checks the limit again.
    if non_null_count > T::MAX_LIST_LEN {
        return Ok(None);
    }

    Ok(Some(Arc::new(BranchlessFilter::<T>::try_new(in_array)?)))
}

#[cfg(test)]
mod tests {
    use arrow::array::{Decimal128Array, UInt32Array};
    use arrow::datatypes::UInt32Type;

    use super::super::branchless_filter::BranchlessFilterType;
    use super::*;

    fn uint32_array(values: Vec<Option<u32>>) -> ArrayRef {
        Arc::new(UInt32Array::from(values))
    }

    #[test]
    fn branchless_routing_respects_max_list_len() -> Result<()> {
        let max_len = <UInt32Type as BranchlessFilterType>::MAX_LIST_LEN;

        let values = (0..max_len)
            .map(|value| Some(value as u32))
            .collect::<Vec<_>>();
        assert!(
            branchless_filter::<UInt32Type>(&uint32_array(values), max_len)?.is_some()
        );

        let values = (0..=max_len)
            .map(|value| Some(value as u32))
            .collect::<Vec<_>>();
        assert!(
            branchless_filter::<UInt32Type>(&uint32_array(values), max_len + 1)?
                .is_none()
        );

        Ok(())
    }

    #[test]
    fn primitive_frozen_routing_starts_after_max_list_len() -> Result<()> {
        let max_len = <UInt32Type as BranchlessFilterType>::MAX_LIST_LEN;

        let values = (0..max_len)
            .map(|value| Some(value as u32))
            .collect::<Vec<_>>();
        assert!(
            primitive_frozen_filter::<UInt32Type>(&uint32_array(values), max_len)?
                .is_none()
        );

        let values = (0..=max_len)
            .map(|value| Some(value as u32))
            .collect::<Vec<_>>();
        assert!(
            primitive_frozen_filter::<UInt32Type>(&uint32_array(values), max_len + 1)?
                .is_some()
        );

        Ok(())
    }

    #[test]
    fn primitive_frozen_routing_excludes_128_bit_values() -> Result<()> {
        let array: ArrayRef = Arc::new(Decimal128Array::from(vec![1, 2, 3, 4, 5]));
        assert!(instantiate_primitive_filter(&array)?.is_none());
        Ok(())
    }

    #[test]
    fn branchless_routing_handles_zero_non_null_values() -> Result<()> {
        let array = uint32_array(vec![None; 3]);

        assert!(branchless_filter::<UInt32Type>(&array, 0)?.is_some());

        Ok(())
    }
}
