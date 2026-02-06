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

//! Type variation constants
//!
//! To add support for types not in the [core specification](https://substrait.io/types/type_classes/),
//! we make use of the [simple extensions] of substrait type. This module contains the constants used
//! to identify the type variation.
//!
//! The rules of type variations here are:
//! - Default type reference is 0. It is used when the actual type is the same with the original type.
//! - Extended variant type references start from 1, and usually increase by 1.
//!
//! TODO: Definitions here are not the final form. All the non-system-preferred variations will be defined
//! using [simple extensions] as per the [spec of type_variations](https://substrait.io/types/type_variations/)
//! <https://github.com/apache/datafusion/issues/11545>
//!
//! [simple extensions]: (https://substrait.io/extensions/#simple-extensions)

// For [type variations](https://substrait.io/types/type_variations/#type-variations) in substrait.
// Type variations are used to represent different types based on one type class.
// TODO: Define as extensions: <https://github.com/apache/datafusion/issues/11544>

/// The "system-preferred" variation (i.e., no variation).
pub const DEFAULT_TYPE_VARIATION_REF: u32 = 0;
#[deprecated(
    since = "46.0.0",
    note = "Use Arrow extension types (u8, u16, u32, u64) instead"
)]
pub const UNSIGNED_INTEGER_TYPE_VARIATION_REF: u32 = 1;

#[deprecated(since = "42.0.0", note = "Use `PrecisionTimestamp(Tz)` type instead")]
pub const TIMESTAMP_SECOND_TYPE_VARIATION_REF: u32 = 0;
#[deprecated(since = "42.0.0", note = "Use `PrecisionTimestamp(Tz)` type instead")]
pub const TIMESTAMP_MILLI_TYPE_VARIATION_REF: u32 = 1;
#[deprecated(since = "42.0.0", note = "Use `PrecisionTimestamp(Tz)` type instead")]
pub const TIMESTAMP_MICRO_TYPE_VARIATION_REF: u32 = 2;
#[deprecated(since = "42.0.0", note = "Use `PrecisionTimestamp(Tz)` type instead")]
pub const TIMESTAMP_NANO_TYPE_VARIATION_REF: u32 = 3;

pub const DATE_32_TYPE_VARIATION_REF: u32 = 0;
#[deprecated(since = "46.0.0", note = "Use Arrow extension type (date_millis) instead")]
pub const DATE_64_TYPE_VARIATION_REF: u32 = 1;
#[deprecated(
    since = "46.0.0",
    note = "Use Arrow extension types (time_seconds, time_millis) instead"
)]
pub const TIME_32_TYPE_VARIATION_REF: u32 = 0;
#[deprecated(
    since = "46.0.0",
    note = "Use Arrow extension types (time_millis, time_nanos) instead"
)]
pub const TIME_64_TYPE_VARIATION_REF: u32 = 1;
pub const DEFAULT_CONTAINER_TYPE_VARIATION_REF: u32 = 0;
#[deprecated(
    since = "46.0.0",
    note = "Use Arrow extension types (large_string, large_binary, large_list) instead"
)]
pub const LARGE_CONTAINER_TYPE_VARIATION_REF: u32 = 1;
pub const VIEW_CONTAINER_TYPE_VARIATION_REF: u32 = 2;
pub const DEFAULT_MAP_TYPE_VARIATION_REF: u32 = 0;
pub const DICTIONARY_MAP_TYPE_VARIATION_REF: u32 = 1;
pub const DECIMAL_128_TYPE_VARIATION_REF: u32 = 0;
#[deprecated(since = "46.0.0", note = "Use Arrow extension type (decimal256) instead")]
pub const DECIMAL_256_TYPE_VARIATION_REF: u32 = 1;
/// Used for the arrow type [`DataType::Interval`] with [`IntervalUnit::DayTime`].
///
/// [`DataType::Interval`]: datafusion::arrow::datatypes::DataType::Interval
/// [`IntervalUnit::DayTime`]: datafusion::arrow::datatypes::IntervalUnit::DayTime
pub const DEFAULT_INTERVAL_DAY_TYPE_VARIATION_REF: u32 = 0;
/// Used for the arrow type [`DataType::Duration`].
///
/// [`DataType::Duration`]: datafusion::arrow::datatypes::DataType::Duration
#[deprecated(since = "46.0.0", note = "Use Arrow extension type (duration) instead")]
pub const DURATION_INTERVAL_DAY_TYPE_VARIATION_REF: u32 = 1;

// For [user-defined types](https://substrait.io/types/type_classes/#user-defined-types).
/// For [`DataType::Interval`] with [`IntervalUnit::YearMonth`].
///
/// An `i32` for elapsed whole months. See also [`ScalarValue::IntervalYearMonth`]
/// for the literal definition in DataFusion.
///
/// [`DataType::Interval`]: datafusion::arrow::datatypes::DataType::Interval
/// [`IntervalUnit::YearMonth`]: datafusion::arrow::datatypes::IntervalUnit::YearMonth
/// [`ScalarValue::IntervalYearMonth`]: datafusion::common::ScalarValue::IntervalYearMonth
#[deprecated(since = "41.0.0", note = "Use Substrait `IntervalYear` type instead")]
pub const INTERVAL_YEAR_MONTH_TYPE_REF: u32 = 1;

/// For [`DataType::Interval`] with [`IntervalUnit::DayTime`].
///
/// An `i64` as:
/// - days: `i32`
/// - milliseconds: `i32`
///
/// See also [`ScalarValue::IntervalDayTime`] for the literal definition in DataFusion.
///
/// [`DataType::Interval`]: datafusion::arrow::datatypes::DataType::Interval
/// [`IntervalUnit::DayTime`]: datafusion::arrow::datatypes::IntervalUnit::DayTime
/// [`ScalarValue::IntervalDayTime`]: datafusion::common::ScalarValue::IntervalDayTime
#[deprecated(since = "41.0.0", note = "Use Substrait `IntervalDay` type instead")]
pub const INTERVAL_DAY_TIME_TYPE_REF: u32 = 2;

/// For [`DataType::Interval`] with [`IntervalUnit::MonthDayNano`].
///
/// An `i128` as:
/// - months: `i32`
/// - days: `i32`
/// - nanoseconds: `i64`
///
/// See also [`ScalarValue::IntervalMonthDayNano`] for the literal definition in DataFusion.
///
/// [`DataType::Interval`]: datafusion::arrow::datatypes::DataType::Interval
/// [`IntervalUnit::MonthDayNano`]: datafusion::arrow::datatypes::IntervalUnit::MonthDayNano
/// [`ScalarValue::IntervalMonthDayNano`]: datafusion::common::ScalarValue::IntervalMonthDayNano
#[deprecated(
    since = "41.0.0",
    note = "Use Substrait `IntervalCompound` type instead"
)]
pub const INTERVAL_MONTH_DAY_NANO_TYPE_REF: u32 = 3;

/// For [`DataType::Interval`] with [`IntervalUnit::MonthDayNano`].
///
/// [`DataType::Interval`]: datafusion::arrow::datatypes::DataType::Interval
/// [`IntervalUnit::MonthDayNano`]: datafusion::arrow::datatypes::IntervalUnit::MonthDayNano
#[deprecated(
    since = "43.0.0",
    note = "Use Substrait `IntervalCompound` type instead"
)]
pub const INTERVAL_MONTH_DAY_NANO_TYPE_NAME: &str = "interval-month-day-nano";

/// Defined in <https://github.com/apache/arrow/blame/main/format/substrait/extension_types.yaml>
pub const FLOAT_16_TYPE_NAME: &str = "fp16";

/// For [`DataType::Null`]
///
/// [`DataType::Null`]: datafusion::arrow::datatypes::DataType::Null
pub const NULL_TYPE_NAME: &str = "null";

// Unsigned integer type names as defined in Arrow's extension_types.yaml
// See: <https://github.com/apache/arrow/blob/main/format/substrait/extension_types.yaml>

/// For [`DataType::UInt8`]
///
/// [`DataType::UInt8`]: datafusion::arrow::datatypes::DataType::UInt8
pub const U8_TYPE_NAME: &str = "u8";

/// For [`DataType::UInt16`]
///
/// [`DataType::UInt16`]: datafusion::arrow::datatypes::DataType::UInt16
pub const U16_TYPE_NAME: &str = "u16";

/// For [`DataType::UInt32`]
///
/// [`DataType::UInt32`]: datafusion::arrow::datatypes::DataType::UInt32
pub const U32_TYPE_NAME: &str = "u32";

/// For [`DataType::UInt64`]
///
/// [`DataType::UInt64`]: datafusion::arrow::datatypes::DataType::UInt64
pub const U64_TYPE_NAME: &str = "u64";

// Large container type names as defined in Arrow's extension_types.yaml

/// For [`DataType::LargeUtf8`]
///
/// [`DataType::LargeUtf8`]: datafusion::arrow::datatypes::DataType::LargeUtf8
pub const LARGE_STRING_TYPE_NAME: &str = "large_string";

/// For [`DataType::LargeBinary`]
///
/// [`DataType::LargeBinary`]: datafusion::arrow::datatypes::DataType::LargeBinary
pub const LARGE_BINARY_TYPE_NAME: &str = "large_binary";

/// For [`DataType::LargeList`]
///
/// [`DataType::LargeList`]: datafusion::arrow::datatypes::DataType::LargeList
pub const LARGE_LIST_TYPE_NAME: &str = "large_list";

/// For [`DataType::Decimal256`]
///
/// [`DataType::Decimal256`]: datafusion::arrow::datatypes::DataType::Decimal256
pub const DECIMAL256_TYPE_NAME: &str = "decimal256";

/// For [`DataType::Duration`]
///
/// [`DataType::Duration`]: datafusion::arrow::datatypes::DataType::Duration
pub const DURATION_TYPE_NAME: &str = "duration";

// Date/Time type names as defined in Arrow's extension_types.yaml

/// For [`DataType::Date64`]
///
/// [`DataType::Date64`]: datafusion::arrow::datatypes::DataType::Date64
pub const DATE_MILLIS_TYPE_NAME: &str = "date_millis";

/// For [`DataType::Time32`] with [`TimeUnit::Second`]
///
/// [`DataType::Time32`]: datafusion::arrow::datatypes::DataType::Time32
/// [`TimeUnit::Second`]: datafusion::arrow::datatypes::TimeUnit::Second
pub const TIME_SECONDS_TYPE_NAME: &str = "time_seconds";

/// For [`DataType::Time32`] with [`TimeUnit::Millisecond`]
///
/// [`DataType::Time32`]: datafusion::arrow::datatypes::DataType::Time32
/// [`TimeUnit::Millisecond`]: datafusion::arrow::datatypes::TimeUnit::Millisecond
pub const TIME_MILLIS_TYPE_NAME: &str = "time_millis";

/// For [`DataType::Time64`] with [`TimeUnit::Nanosecond`]
///
/// [`DataType::Time64`]: datafusion::arrow::datatypes::DataType::Time64
/// [`TimeUnit::Nanosecond`]: datafusion::arrow::datatypes::TimeUnit::Nanosecond
pub const TIME_NANOS_TYPE_NAME: &str = "time_nanos";

/// For [`DataType::FixedSizeList`]
///
/// [`DataType::FixedSizeList`]: datafusion::arrow::datatypes::DataType::FixedSizeList
pub const FIXED_SIZE_LIST_TYPE_NAME: &str = "fixed_size_list";
