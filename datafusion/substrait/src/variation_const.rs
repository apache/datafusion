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
//! we make use of the [simple extensions](https://substrait.io/extensions/#simple-extensions) of substrait
//! type. This module contains the constants used to identify the type variation.
//!
//! The rules of type variations here are:
//! - Default type reference is 0. It is used when the actual type is the same with the original type.
//! - Extended variant type references start from 1, and ususlly increase by 1.

// For type variations
pub const DEFAULT_TYPE_REF: u32 = 0;
pub const UNSIGNED_INTEGER_TYPE_REF: u32 = 1;
pub const TIMESTAMP_SECOND_TYPE_REF: u32 = 0;
pub const TIMESTAMP_MILLI_TYPE_REF: u32 = 1;
pub const TIMESTAMP_MICRO_TYPE_REF: u32 = 2;
pub const TIMESTAMP_NANO_TYPE_REF: u32 = 3;
pub const DATE_32_TYPE_REF: u32 = 0;
pub const DATE_64_TYPE_REF: u32 = 1;
pub const DEFAULT_CONTAINER_TYPE_REF: u32 = 0;
pub const LARGE_CONTAINER_TYPE_REF: u32 = 1;
pub const DECIMAL_128_TYPE_REF: u32 = 0;
pub const DECIMAL_256_TYPE_REF: u32 = 1;

// For custom types
/// For [`DataType::Interval`] with [`IntervalUnit::YearMonth`].
///
/// An `i32` for elapsed whole months. See also [`ScalarValue::IntervalYearMonth`]
/// for the literal definition in DataFusion.
///
/// [`DataType::Interval`]: datafusion::arrow::datatypes::DataType::Interval
/// [`IntervalUnit::YearMonth`]: datafusion::arrow::datatypes::IntervalUnit::YearMonth
/// [`ScalarValue::IntervalYearMonth`]: datafusion::common::ScalarValue::IntervalYearMonth
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
pub const INTERVAL_MONTH_DAY_NANO_TYPE_REF: u32 = 3;

// For User Defined URLs
/// For [`DataType::Interval`] with [`IntervalUnit::YearMonth`].
///
/// [`DataType::Interval`]: datafusion::arrow::datatypes::DataType::Interval
/// [`IntervalUnit::YearMonth`]: datafusion::arrow::datatypes::IntervalUnit::YearMonth
pub const INTERVAL_YEAR_MONTH_TYPE_URL: &str = "interval-year-month";
/// For [`DataType::Interval`] with [`IntervalUnit::DayTime`].
///
/// [`DataType::Interval`]: datafusion::arrow::datatypes::DataType::Interval
/// [`IntervalUnit::DayTime`]: datafusion::arrow::datatypes::IntervalUnit::DayTime
pub const INTERVAL_DAY_TIME_TYPE_URL: &str = "interval-day-time";
/// For [`DataType::Interval`] with [`IntervalUnit::MonthDayNano`].
///
/// [`DataType::Interval`]: datafusion::arrow::datatypes::DataType::Interval
/// [`IntervalUnit::MonthDayNano`]: datafusion::arrow::datatypes::IntervalUnit::MonthDayNano
pub const INTERVAL_MONTH_DAY_NANO_TYPE_URL: &str = "interval-month-day-nano";
