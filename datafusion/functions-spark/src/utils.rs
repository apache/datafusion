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

use arrow_array::{
    cast::as_primitive_array,
    types::{Int32Type, TimestampMicrosecondType},
};
use arrow_schema::{ArrowError, DataType, TimeUnit, DECIMAL128_MAX_PRECISION};
use std::sync::Arc;

use crate::timezone::Tz;
use arrow::{
    array::{as_dictionary_array, Array, ArrayRef, PrimitiveArray},
    temporal_conversions::as_datetime,
};
use arrow_array::types::TimestampMillisecondType;
use arrow_data::decimal::{MAX_DECIMAL_FOR_EACH_PRECISION, MIN_DECIMAL_FOR_EACH_PRECISION};
use chrono::{DateTime, Offset, TimeZone};

/// Preprocesses input arrays to add timezone information from Spark to Arrow array datatype or
/// to apply timezone offset.
//
//  We consider the following cases:
//
//  | --------------------- | ------------ | ----------------- | -------------------------------- |
//  | Conversion            | Input array  | Timezone          | Output array                     |
//  | --------------------- | ------------ | ----------------- | -------------------------------- |
//  | Timestamp ->          | Array in UTC | Timezone of input | A timestamp with the timezone    |
//  |  Utf8 or Date32       |              |                   | offset applied and timezone      |
//  |                       |              |                   | removed                          |
//  | --------------------- | ------------ | ----------------- | -------------------------------- |
//  | Timestamp ->          | Array in UTC | Timezone of input | Same as input array              |
//  |  Timestamp  w/Timezone|              |                   |                                  |
//  | --------------------- | ------------ | ----------------- | -------------------------------- |
//  | Timestamp_ntz ->      | Array in     | Timezone of input | Same as input array              |
//  |   Utf8 or Date32      | timezone     |                   |                                  |
//  |                       | session local|                   |                                  |
//  |                       | timezone     |                   |                                  |
//  | --------------------- | ------------ | ----------------- | -------------------------------- |
//  | Timestamp_ntz ->      | Array in     | Timezone of input |  Array in UTC and timezone       |
//  |  Timestamp w/Timezone | session local|                   |  specified in input              |
//  |                       | timezone     |                   |                                  |
//  | --------------------- | ------------ | ----------------- | -------------------------------- |
//  | Timestamp(_ntz) ->    |                                                                     |
//  |        Any other type |              Not Supported                                          |
//  | --------------------- | ------------ | ----------------- | -------------------------------- |
//
pub fn array_with_timezone(
    array: ArrayRef,
    timezone: String,
    to_type: Option<&DataType>,
) -> Result<ArrayRef, ArrowError> {
    match array.data_type() {
        DataType::Timestamp(_, None) => {
            assert!(!timezone.is_empty());
            match to_type {
                Some(DataType::Utf8) | Some(DataType::Date32) => Ok(array),
                Some(DataType::Timestamp(_, Some(_))) => {
                    timestamp_ntz_to_timestamp(array, timezone.as_str(), Some(timezone.as_str()))
                }
                Some(DataType::Timestamp(_, None)) => {
                    timestamp_ntz_to_timestamp(array, timezone.as_str(), None)
                }
                _ => {
                    // Not supported
                    panic!(
                        "Cannot convert from {:?} to {:?}",
                        array.data_type(),
                        to_type.unwrap()
                    )
                }
            }
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
            assert!(!timezone.is_empty());
            let array = as_primitive_array::<TimestampMicrosecondType>(&array);
            let array_with_timezone = array.clone().with_timezone(timezone.clone());
            let array = Arc::new(array_with_timezone) as ArrayRef;
            match to_type {
                Some(DataType::Utf8) | Some(DataType::Date32) => {
                    pre_timestamp_cast(array, timezone)
                }
                _ => Ok(array),
            }
        }
        DataType::Timestamp(TimeUnit::Millisecond, Some(_)) => {
            assert!(!timezone.is_empty());
            let array = as_primitive_array::<TimestampMillisecondType>(&array);
            let array_with_timezone = array.clone().with_timezone(timezone.clone());
            let array = Arc::new(array_with_timezone) as ArrayRef;
            match to_type {
                Some(DataType::Utf8) | Some(DataType::Date32) => {
                    pre_timestamp_cast(array, timezone)
                }
                _ => Ok(array),
            }
        }
        DataType::Dictionary(_, value_type)
            if matches!(value_type.as_ref(), &DataType::Timestamp(_, _)) =>
        {
            let dict = as_dictionary_array::<Int32Type>(&array);
            let array = as_primitive_array::<TimestampMicrosecondType>(dict.values());
            let array_with_timezone =
                array_with_timezone(Arc::new(array.clone()) as ArrayRef, timezone, to_type)?;
            let dict = dict.with_values(array_with_timezone);
            Ok(Arc::new(dict))
        }
        _ => Ok(array),
    }
}

fn datetime_cast_err(value: i64) -> ArrowError {
    ArrowError::CastError(format!(
        "Cannot convert TimestampMicrosecondType {value} to datetime. Comet only supports dates between Jan 1, 262145 BCE and Dec 31, 262143 CE",
    ))
}

/// Takes in a Timestamp(Microsecond, None) array and a timezone id, and returns
/// a Timestamp(Microsecond, Some<_>) array.
/// The understanding is that the input array has time in the timezone specified in the second
/// argument.
/// Parameters:
///     array - input array of timestamp without timezone
///     tz - timezone of the values in the input array
///     to_timezone - timezone to change the input values to
fn timestamp_ntz_to_timestamp(
    array: ArrayRef,
    tz: &str,
    to_timezone: Option<&str>,
) -> Result<ArrayRef, ArrowError> {
    assert!(!tz.is_empty());
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let array = as_primitive_array::<TimestampMicrosecondType>(&array);
            let tz: Tz = tz.parse()?;
            let array: PrimitiveArray<TimestampMicrosecondType> = array.try_unary(|value| {
                as_datetime::<TimestampMicrosecondType>(value)
                    .ok_or_else(|| datetime_cast_err(value))
                    .map(|local_datetime| {
                        let datetime: DateTime<Tz> =
                            tz.from_local_datetime(&local_datetime).unwrap();
                        datetime.timestamp_micros()
                    })
            })?;
            let array_with_tz = if let Some(to_tz) = to_timezone {
                array.with_timezone(to_tz)
            } else {
                array
            };
            Ok(Arc::new(array_with_tz))
        }
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            let array = as_primitive_array::<TimestampMillisecondType>(&array);
            let tz: Tz = tz.parse()?;
            let array: PrimitiveArray<TimestampMillisecondType> = array.try_unary(|value| {
                as_datetime::<TimestampMillisecondType>(value)
                    .ok_or_else(|| datetime_cast_err(value))
                    .map(|local_datetime| {
                        let datetime: DateTime<Tz> =
                            tz.from_local_datetime(&local_datetime).unwrap();
                        datetime.timestamp_millis()
                    })
            })?;
            let array_with_tz = if let Some(to_tz) = to_timezone {
                array.with_timezone(to_tz)
            } else {
                array
            };
            Ok(Arc::new(array_with_tz))
        }
        _ => Ok(array),
    }
}

/// This takes for special pre-casting cases of Spark. E.g., Timestamp to String.
fn pre_timestamp_cast(array: ArrayRef, timezone: String) -> Result<ArrayRef, ArrowError> {
    assert!(!timezone.is_empty());
    match array.data_type() {
        DataType::Timestamp(_, _) => {
            // Spark doesn't output timezone while casting timestamp to string, but arrow's cast
            // kernel does if timezone exists. So we need to apply offset of timezone to array
            // timestamp value and remove timezone from array datatype.
            let array = as_primitive_array::<TimestampMicrosecondType>(&array);

            let tz: Tz = timezone.parse()?;
            let array: PrimitiveArray<TimestampMicrosecondType> = array.try_unary(|value| {
                as_datetime::<TimestampMicrosecondType>(value)
                    .ok_or_else(|| datetime_cast_err(value))
                    .map(|datetime| {
                        let offset = tz.offset_from_utc_datetime(&datetime).fix();
                        let datetime = datetime + offset;
                        datetime.and_utc().timestamp_micros()
                    })
            })?;

            Ok(Arc::new(array))
        }
        _ => Ok(array),
    }
}

/// Adapted from arrow-rs `validate_decimal_precision` but returns bool
/// instead of Err to avoid the cost of formatting the error strings and is
/// optimized to remove a memcpy that exists in the original function
/// we can remove this code once we upgrade to a version of arrow-rs that
/// includes https://github.com/apache/arrow-rs/pull/6419
#[inline]
pub fn is_valid_decimal_precision(value: i128, precision: u8) -> bool {
    precision <= DECIMAL128_MAX_PRECISION
        && value >= MIN_DECIMAL_FOR_EACH_PRECISION[precision as usize - 1]
        && value <= MAX_DECIMAL_FOR_EACH_PRECISION[precision as usize - 1]
}

// These are borrowed from hashbrown crate:
//   https://github.com/rust-lang/hashbrown/blob/master/src/raw/mod.rs

// On stable we can use #[cold] to get a equivalent effect: this attributes
// suggests that the function is unlikely to be called
#[inline]
#[cold]
pub fn cold() {}

#[inline]
pub fn likely(b: bool) -> bool {
    if !b {
        cold();
    }
    b
}
#[inline]
pub fn unlikely(b: bool) -> bool {
    if b {
        cold();
    }
    b
}
