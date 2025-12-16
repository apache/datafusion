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

use arrow::datatypes::IntervalUnit::*;
use arrow::datatypes::TimeUnit::*;

use crate::types::{LogicalTypeRef, NativeType};
use std::sync::{Arc, LazyLock};

/// Create a singleton and accompanying static variable for a [`LogicalTypeRef`]
/// of a [`NativeType`].
/// * `name`: name of the static variable, must be unique.
/// * `getter`: name of the public function that will return the singleton instance
///   of the static variable.
/// * `ty`: the [`NativeType`].
macro_rules! singleton {
    ($name:ident, $getter:ident, $ty:ident) => {
        static $name: LazyLock<LogicalTypeRef> =
            LazyLock::new(|| Arc::new(NativeType::$ty));

        #[doc = "Getter for singleton instance of a logical type representing"]
        #[doc = concat!("[`NativeType::", stringify!($ty), "`].")]
        pub fn $getter() -> LogicalTypeRef {
            Arc::clone(&$name)
        }
    };
}

/// Similar to [`singleton`], but for native types that have variants, such as
/// `NativeType::Interval(MonthDayNano)`.
/// * `name`: name of the static variable, must be unique.
/// * `getter`: name of the public function that will return the singleton instance
///   of the static variable.
/// * `ty`: the [`NativeType`].
/// * `variant`: specific variant of the `ty`.
macro_rules! singleton_variant {
    ($name:ident, $getter:ident, $ty:ident, $variant:ident) => {
        static $name: LazyLock<LogicalTypeRef> =
            LazyLock::new(|| Arc::new(NativeType::$ty($variant)));

        #[doc = "Getter for singleton instance of a logical type representing"]
        #[doc = concat!("[`NativeType::", stringify!($ty), "`] of unit [`", stringify!($variant),"`].`")]
        pub fn $getter() -> LogicalTypeRef {
            Arc::clone(&$name)
        }
    };
}

singleton!(LOGICAL_NULL, logical_null, Null);
singleton!(LOGICAL_BOOLEAN, logical_boolean, Boolean);
singleton!(LOGICAL_INT8, logical_int8, Int8);
singleton!(LOGICAL_INT16, logical_int16, Int16);
singleton!(LOGICAL_INT32, logical_int32, Int32);
singleton!(LOGICAL_INT64, logical_int64, Int64);
singleton!(LOGICAL_UINT8, logical_uint8, UInt8);
singleton!(LOGICAL_UINT16, logical_uint16, UInt16);
singleton!(LOGICAL_UINT32, logical_uint32, UInt32);
singleton!(LOGICAL_UINT64, logical_uint64, UInt64);
singleton!(LOGICAL_FLOAT16, logical_float16, Float16);
singleton!(LOGICAL_FLOAT32, logical_float32, Float32);
singleton!(LOGICAL_FLOAT64, logical_float64, Float64);
singleton!(LOGICAL_DATE, logical_date, Date);
singleton!(LOGICAL_BINARY, logical_binary, Binary);
singleton!(LOGICAL_STRING, logical_string, String);

singleton_variant!(
    LOGICAL_INTERVAL_MDN,
    logical_interval_mdn,
    Interval,
    MonthDayNano
);

singleton_variant!(
    LOGICAL_INTERVAL_YEAR_MONTH,
    logical_interval_year_month,
    Interval,
    YearMonth
);

singleton_variant!(
    LOGICAL_DURATION_MICROSECOND,
    logical_duration_microsecond,
    Duration,
    Microsecond
);
