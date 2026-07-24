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

    if let Some(filter) = instantiate_branchless_filter(&in_array)? {
        return Ok(filter);
    }

    if let Some(filter) = instantiate_primitive_frozen_filter(&in_array)? {
        return Ok(filter);
    }

    instantiate_standard_filter(in_array)
}

fn dictionary_value_type(mut data_type: &DataType) -> &DataType {
    while let DataType::Dictionary(_, value_type) = data_type {
        data_type = value_type;
    }
    data_type
}

fn flatten_dictionary_haystack(in_array: ArrayRef) -> Result<ArrayRef> {
    // Flatten dictionary-encoded haystacks to their value type so that
    // specialized filters (e.g. Int32StaticFilter) are used instead of
    // falling through to the generic ArrayStaticFilter.
    match in_array.data_type() {
        DataType::Dictionary(_, value_type) => Ok(cast(&in_array, value_type.as_ref())?),
        _ => Ok(in_array),
    }
}

fn instantiate_branchless_filter(in_array: &ArrayRef) -> Result<Option<StaticFilterRef>> {
    let non_null_count = in_array.len() - in_array.null_count();

    macro_rules! filter {
        ($arrow_type:ty) => {
            branchless_filter::<$arrow_type>(in_array, non_null_count)
        };
    }

    match in_array.data_type() {
        DataType::Int8 => filter!(Int8Type),
        DataType::UInt8 => filter!(UInt8Type),
        DataType::Int16 => filter!(Int16Type),
        DataType::UInt16 => filter!(UInt16Type),
        DataType::Float16 => filter!(Float16Type),
        DataType::Int32 => filter!(Int32Type),
        DataType::UInt32 => filter!(UInt32Type),
        DataType::Float32 => filter!(Float32Type),
        DataType::Date32 => filter!(Date32Type),
        DataType::Time32(unit) => match unit {
            TimeUnit::Second => filter!(Time32SecondType),
            TimeUnit::Millisecond => filter!(Time32MillisecondType),
            _ => Ok(None),
        },
        DataType::Int64 => filter!(Int64Type),
        DataType::UInt64 => filter!(UInt64Type),
        DataType::Float64 => filter!(Float64Type),
        DataType::Date64 => filter!(Date64Type),
        DataType::Time64(unit) => match unit {
            TimeUnit::Microsecond => filter!(Time64MicrosecondType),
            TimeUnit::Nanosecond => filter!(Time64NanosecondType),
            _ => Ok(None),
        },
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => filter!(TimestampSecondType),
            TimeUnit::Millisecond => filter!(TimestampMillisecondType),
            TimeUnit::Microsecond => filter!(TimestampMicrosecondType),
            TimeUnit::Nanosecond => filter!(TimestampNanosecondType),
        },
        DataType::Duration(unit) => match unit {
            TimeUnit::Second => filter!(DurationSecondType),
            TimeUnit::Millisecond => filter!(DurationMillisecondType),
            TimeUnit::Microsecond => filter!(DurationMicrosecondType),
            TimeUnit::Nanosecond => filter!(DurationNanosecondType),
        },
        DataType::Decimal128(_, _) => filter!(Decimal128Type),
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            filter!(IntervalMonthDayNanoType)
        }
        _ => Ok(None),
    }
}

fn instantiate_standard_filter(in_array: ArrayRef) -> Result<StaticFilterRef> {
    match in_array.data_type() {
        DataType::Int8 => bitmap_filter::<Int8Type>(&in_array),
        DataType::UInt8 => bitmap_filter::<UInt8Type>(&in_array),
        DataType::Int16 => bitmap_filter::<Int16Type>(&in_array),
        DataType::UInt16 => bitmap_filter::<UInt16Type>(&in_array),
        DataType::Float16 => bitmap_filter::<Float16Type>(&in_array),
        DataType::Int32 => Ok(Arc::new(Int32StaticFilter::try_new(&in_array)?)),
        DataType::Int64 => Ok(Arc::new(Int64StaticFilter::try_new(&in_array)?)),
        DataType::UInt32 => Ok(Arc::new(UInt32StaticFilter::try_new(&in_array)?)),
        DataType::UInt64 => Ok(Arc::new(UInt64StaticFilter::try_new(&in_array)?)),
        // Float primitive types (use ordered wrappers for Hash/Eq)
        DataType::Float32 => Ok(Arc::new(Float32StaticFilter::try_new(&in_array)?)),
        DataType::Float64 => Ok(Arc::new(Float64StaticFilter::try_new(&in_array)?)),
        _ => {
            // Fall through to generic implementation for unsupported types
            // (Struct, etc.).
            Ok(Arc::new(ArrayStaticFilter::try_new(in_array)?))
        }
    }
}

fn instantiate_primitive_frozen_filter(
    in_array: &ArrayRef,
) -> Result<Option<StaticFilterRef>> {
    let non_null_count = in_array.len() - in_array.null_count();

    macro_rules! filter {
        ($arrow_type:ty) => {
            primitive_frozen_filter::<$arrow_type>(in_array, non_null_count)
        };
    }

    match in_array.data_type() {
        DataType::Int32 => filter!(Int32Type),
        DataType::UInt32 => filter!(UInt32Type),
        DataType::Float32 => filter!(Float32Type),
        DataType::Date32 => filter!(Date32Type),
        DataType::Time32(unit) => match unit {
            TimeUnit::Second => filter!(Time32SecondType),
            TimeUnit::Millisecond => filter!(Time32MillisecondType),
            _ => Ok(None),
        },
        DataType::Int64 => filter!(Int64Type),
        DataType::UInt64 => filter!(UInt64Type),
        DataType::Float64 => filter!(Float64Type),
        DataType::Date64 => filter!(Date64Type),
        DataType::Time64(unit) => match unit {
            TimeUnit::Microsecond => filter!(Time64MicrosecondType),
            TimeUnit::Nanosecond => filter!(Time64NanosecondType),
            _ => Ok(None),
        },
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => filter!(TimestampSecondType),
            TimeUnit::Millisecond => filter!(TimestampMillisecondType),
            TimeUnit::Microsecond => filter!(TimestampMicrosecondType),
            TimeUnit::Nanosecond => filter!(TimestampNanosecondType),
        },
        DataType::Duration(unit) => match unit {
            TimeUnit::Second => filter!(DurationSecondType),
            TimeUnit::Millisecond => filter!(DurationMillisecondType),
            TimeUnit::Microsecond => filter!(DurationMicrosecondType),
            TimeUnit::Nanosecond => filter!(DurationNanosecondType),
        },
        _ => Ok(None),
    }
}

fn bitmap_filter<T>(in_array: &ArrayRef) -> Result<StaticFilterRef>
where
    T: BitmapFilterType,
{
    Ok(Arc::new(BitmapFilter::<T>::try_new(in_array)?))
}

fn primitive_frozen_filter<T>(
    in_array: &ArrayRef,
    non_null_count: usize,
) -> Result<Option<StaticFilterRef>>
where
    T: PrimitiveFilterType,
    PrimitiveFilterNative<T>: Copy + Eq + Hash + Send + Sync,
{
    if non_null_count <= T::BRANCHLESS_MAX_LIST_LEN {
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
    T: PrimitiveFilterType,
    PrimitiveFilterNative<T>: Copy + PartialEq + Send + Sync,
{
    if non_null_count > T::BRANCHLESS_MAX_LIST_LEN {
        return Ok(None);
    }

    branchless_filter_for_len::<T>(in_array, non_null_count).map(Some)
}

fn branchless_filter_for_len<T>(
    in_array: &ArrayRef,
    non_null_count: usize,
) -> Result<StaticFilterRef>
where
    T: PrimitiveFilterType,
    PrimitiveFilterNative<T>: Copy + PartialEq + Send + Sync,
{
    macro_rules! dispatch {
        ($($n:literal),* $(,)?) => {
            match non_null_count {
                $($n => Ok(Arc::new(BranchlessFilter::<T, $n>::try_new(in_array)?)),)*
                _ => unreachable!("validated branchless list length"),
            }
        };
    }

    match T::BRANCHLESS_MAX_LIST_LEN {
        4 => dispatch!(0, 1, 2, 3, 4),
        8 => dispatch!(0, 1, 2, 3, 4, 5, 6, 7, 8),
        16 => dispatch!(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16),
        32 => dispatch!(
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
            22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32,
        ),
        _ => unreachable!("known branchless max list length"),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Decimal128Array, UInt32Array};
    use arrow::datatypes::UInt32Type;

    use super::super::primitive_filter::PrimitiveFilterType;
    use super::*;

    fn uint32_array(values: Vec<Option<u32>>) -> ArrayRef {
        Arc::new(UInt32Array::from(values))
    }

    #[test]
    fn branchless_routing_respects_max_list_len() -> Result<()> {
        let max_len = <UInt32Type as PrimitiveFilterType>::BRANCHLESS_MAX_LIST_LEN;

        let values = (0..max_len)
            .map(|value| Some(value as u32))
            .collect::<Vec<_>>();
        assert!(instantiate_branchless_filter(&uint32_array(values))?.is_some());

        let values = (0..=max_len)
            .map(|value| Some(value as u32))
            .collect::<Vec<_>>();
        assert!(instantiate_branchless_filter(&uint32_array(values))?.is_none());

        Ok(())
    }

    #[test]
    fn primitive_frozen_routing_starts_after_max_list_len() -> Result<()> {
        let max_len = <UInt32Type as PrimitiveFilterType>::BRANCHLESS_MAX_LIST_LEN;

        let values = (0..max_len)
            .map(|value| Some(value as u32))
            .collect::<Vec<_>>();
        assert!(instantiate_primitive_frozen_filter(&uint32_array(values))?.is_none());

        let values = (0..=max_len)
            .map(|value| Some(value as u32))
            .collect::<Vec<_>>();
        assert!(instantiate_primitive_frozen_filter(&uint32_array(values))?.is_some());

        Ok(())
    }

    #[test]
    fn primitive_frozen_routing_excludes_128_bit_values() -> Result<()> {
        let array: ArrayRef = Arc::new(Decimal128Array::from(vec![1, 2, 3, 4, 5]));
        assert!(instantiate_primitive_frozen_filter(&array)?.is_none());
        Ok(())
    }

    #[test]
    fn branchless_routing_handles_zero_non_null_values() -> Result<()> {
        let array = uint32_array(vec![None; 3]);

        assert!(instantiate_branchless_filter(&array)?.is_some());

        Ok(())
    }
}
