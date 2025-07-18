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

//! Basic min/max functionality shared across DataFusion aggregate functions

use arrow::array::{
    ArrayRef, AsArray as _, BinaryArray, BinaryViewArray, BooleanArray, Date32Array,
    Date64Array, Decimal128Array, Decimal256Array, DurationMicrosecondArray,
    DurationMillisecondArray, DurationNanosecondArray, DurationSecondArray,
    FixedSizeBinaryArray, Float16Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, IntervalDayTimeArray, IntervalMonthDayNanoArray,
    IntervalYearMonthArray, LargeBinaryArray, LargeStringArray, StringArray,
    StringViewArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array,
};
use arrow::compute;
use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion_common::{downcast_value, Result, ScalarValue};
use std::cmp::Ordering;

// Statically-typed version of min/max(array) -> ScalarValue for string types
macro_rules! typed_min_max_batch_string {
    ($VALUES:expr, $ARRAYTYPE:ident, $SCALAR:ident, $OP:ident) => {{
        let array = downcast_value!($VALUES, $ARRAYTYPE);
        let value = compute::$OP(array);
        let value = value.and_then(|e| Some(e.to_string()));
        ScalarValue::$SCALAR(value)
    }};
}

// Statically-typed version of min/max(array) -> ScalarValue for binary types.
macro_rules! typed_min_max_batch_binary {
    ($VALUES:expr, $ARRAYTYPE:ident, $SCALAR:ident, $OP:ident) => {{
        let array = downcast_value!($VALUES, $ARRAYTYPE);
        let value = compute::$OP(array);
        let value = value.and_then(|e| Some(e.to_vec()));
        ScalarValue::$SCALAR(value)
    }};
}

// Statically-typed version of min/max(array) -> ScalarValue for non-string types.
macro_rules! typed_min_max_batch {
    ($VALUES:expr, $ARRAYTYPE:ident, $SCALAR:ident, $OP:ident $(, $EXTRA_ARGS:ident)*) => {{
        let array = downcast_value!($VALUES, $ARRAYTYPE);
        let value = compute::$OP(array);
        ScalarValue::$SCALAR(value, $($EXTRA_ARGS.clone()),*)
    }};
}

// Statically-typed version of min/max(array) -> ScalarValue  for non-string types.
// this is a macro to support both operations (min and max).
macro_rules! min_max_batch {
    ($VALUES:expr, $OP:ident) => {{
        match $VALUES.data_type() {
            DataType::Null => ScalarValue::Null,
            DataType::Decimal128(precision, scale) => {
                typed_min_max_batch!(
                    $VALUES,
                    Decimal128Array,
                    Decimal128,
                    $OP,
                    precision,
                    scale
                )
            }
            DataType::Decimal256(precision, scale) => {
                typed_min_max_batch!(
                    $VALUES,
                    Decimal256Array,
                    Decimal256,
                    $OP,
                    precision,
                    scale
                )
            }
            // all types that have a natural order
            DataType::Float64 => {
                typed_min_max_batch!($VALUES, Float64Array, Float64, $OP)
            }
            DataType::Float32 => {
                typed_min_max_batch!($VALUES, Float32Array, Float32, $OP)
            }
            DataType::Float16 => {
                typed_min_max_batch!($VALUES, Float16Array, Float16, $OP)
            }
            DataType::Int64 => typed_min_max_batch!($VALUES, Int64Array, Int64, $OP),
            DataType::Int32 => typed_min_max_batch!($VALUES, Int32Array, Int32, $OP),
            DataType::Int16 => typed_min_max_batch!($VALUES, Int16Array, Int16, $OP),
            DataType::Int8 => typed_min_max_batch!($VALUES, Int8Array, Int8, $OP),
            DataType::UInt64 => typed_min_max_batch!($VALUES, UInt64Array, UInt64, $OP),
            DataType::UInt32 => typed_min_max_batch!($VALUES, UInt32Array, UInt32, $OP),
            DataType::UInt16 => typed_min_max_batch!($VALUES, UInt16Array, UInt16, $OP),
            DataType::UInt8 => typed_min_max_batch!($VALUES, UInt8Array, UInt8, $OP),
            DataType::Timestamp(TimeUnit::Second, tz_opt) => {
                typed_min_max_batch!(
                    $VALUES,
                    TimestampSecondArray,
                    TimestampSecond,
                    $OP,
                    tz_opt
                )
            }
            DataType::Timestamp(TimeUnit::Millisecond, tz_opt) => typed_min_max_batch!(
                $VALUES,
                TimestampMillisecondArray,
                TimestampMillisecond,
                $OP,
                tz_opt
            ),
            DataType::Timestamp(TimeUnit::Microsecond, tz_opt) => typed_min_max_batch!(
                $VALUES,
                TimestampMicrosecondArray,
                TimestampMicrosecond,
                $OP,
                tz_opt
            ),
            DataType::Timestamp(TimeUnit::Nanosecond, tz_opt) => typed_min_max_batch!(
                $VALUES,
                TimestampNanosecondArray,
                TimestampNanosecond,
                $OP,
                tz_opt
            ),
            DataType::Date32 => typed_min_max_batch!($VALUES, Date32Array, Date32, $OP),
            DataType::Date64 => typed_min_max_batch!($VALUES, Date64Array, Date64, $OP),
            DataType::Time32(TimeUnit::Second) => {
                typed_min_max_batch!($VALUES, Time32SecondArray, Time32Second, $OP)
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                typed_min_max_batch!(
                    $VALUES,
                    Time32MillisecondArray,
                    Time32Millisecond,
                    $OP
                )
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                typed_min_max_batch!(
                    $VALUES,
                    Time64MicrosecondArray,
                    Time64Microsecond,
                    $OP
                )
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                typed_min_max_batch!(
                    $VALUES,
                    Time64NanosecondArray,
                    Time64Nanosecond,
                    $OP
                )
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                typed_min_max_batch!(
                    $VALUES,
                    IntervalYearMonthArray,
                    IntervalYearMonth,
                    $OP
                )
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                typed_min_max_batch!($VALUES, IntervalDayTimeArray, IntervalDayTime, $OP)
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                typed_min_max_batch!(
                    $VALUES,
                    IntervalMonthDayNanoArray,
                    IntervalMonthDayNano,
                    $OP
                )
            }
            DataType::Duration(TimeUnit::Second) => {
                typed_min_max_batch!($VALUES, DurationSecondArray, DurationSecond, $OP)
            }
            DataType::Duration(TimeUnit::Millisecond) => {
                typed_min_max_batch!(
                    $VALUES,
                    DurationMillisecondArray,
                    DurationMillisecond,
                    $OP
                )
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                typed_min_max_batch!(
                    $VALUES,
                    DurationMicrosecondArray,
                    DurationMicrosecond,
                    $OP
                )
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                typed_min_max_batch!(
                    $VALUES,
                    DurationNanosecondArray,
                    DurationNanosecond,
                    $OP
                )
            }
            other => {
                // This should have been handled before
                return datafusion_common::internal_err!(
                    "Min/Max accumulator not implemented for type {:?}",
                    other
                );
            }
        }
    }};
}

/// dynamically-typed min(array) -> ScalarValue
pub fn min_batch(values: &ArrayRef) -> Result<ScalarValue> {
    Ok(match values.data_type() {
        DataType::Utf8 => {
            typed_min_max_batch_string!(values, StringArray, Utf8, min_string)
        }
        DataType::LargeUtf8 => {
            typed_min_max_batch_string!(values, LargeStringArray, LargeUtf8, min_string)
        }
        DataType::Utf8View => {
            typed_min_max_batch_string!(
                values,
                StringViewArray,
                Utf8View,
                min_string_view
            )
        }
        DataType::Boolean => {
            typed_min_max_batch!(values, BooleanArray, Boolean, min_boolean)
        }
        DataType::Binary => {
            typed_min_max_batch_binary!(&values, BinaryArray, Binary, min_binary)
        }
        DataType::LargeBinary => {
            typed_min_max_batch_binary!(
                &values,
                LargeBinaryArray,
                LargeBinary,
                min_binary
            )
        }
        DataType::FixedSizeBinary(size) => {
            let array = downcast_value!(&values, FixedSizeBinaryArray);
            let value = compute::min_fixed_size_binary(array);
            let value = value.map(|e| e.to_vec());
            ScalarValue::FixedSizeBinary(*size, value)
        }
        DataType::BinaryView => {
            typed_min_max_batch_binary!(
                &values,
                BinaryViewArray,
                BinaryView,
                min_binary_view
            )
        }
        DataType::Struct(_) => min_max_batch_generic(values, Ordering::Greater)?,
        DataType::List(_) => min_max_batch_generic(values, Ordering::Greater)?,
        DataType::LargeList(_) => min_max_batch_generic(values, Ordering::Greater)?,
        DataType::FixedSizeList(_, _) => {
            min_max_batch_generic(values, Ordering::Greater)?
        }
        DataType::Dictionary(_, _) => {
            let values = values.as_any_dictionary().values();
            min_batch(values)?
        }
        _ => min_max_batch!(values, min),
    })
}

/// Generic min/max implementation for complex types
fn min_max_batch_generic(array: &ArrayRef, ordering: Ordering) -> Result<ScalarValue> {
    if array.len() == array.null_count() {
        return ScalarValue::try_from(array.data_type());
    }
    let mut extreme = ScalarValue::try_from_array(array, 0)?;
    for i in 1..array.len() {
        let current = ScalarValue::try_from_array(array, i)?;
        if current.is_null() {
            continue;
        }
        if extreme.is_null() {
            extreme = current;
            continue;
        }
        let cmp = extreme.try_cmp(&current)?;
        if cmp == ordering {
            extreme = current;
        }
    }

    Ok(extreme)
}

/// dynamically-typed max(array) -> ScalarValue
pub fn max_batch(values: &ArrayRef) -> Result<ScalarValue> {
    Ok(match values.data_type() {
        DataType::Utf8 => {
            typed_min_max_batch_string!(values, StringArray, Utf8, max_string)
        }
        DataType::LargeUtf8 => {
            typed_min_max_batch_string!(values, LargeStringArray, LargeUtf8, max_string)
        }
        DataType::Utf8View => {
            typed_min_max_batch_string!(
                values,
                StringViewArray,
                Utf8View,
                max_string_view
            )
        }
        DataType::Boolean => {
            typed_min_max_batch!(values, BooleanArray, Boolean, max_boolean)
        }
        DataType::Binary => {
            typed_min_max_batch_binary!(&values, BinaryArray, Binary, max_binary)
        }
        DataType::BinaryView => {
            typed_min_max_batch_binary!(
                &values,
                BinaryViewArray,
                BinaryView,
                max_binary_view
            )
        }
        DataType::LargeBinary => {
            typed_min_max_batch_binary!(
                &values,
                LargeBinaryArray,
                LargeBinary,
                max_binary
            )
        }
        DataType::FixedSizeBinary(size) => {
            let array = downcast_value!(&values, FixedSizeBinaryArray);
            let value = compute::max_fixed_size_binary(array);
            let value = value.map(|e| e.to_vec());
            ScalarValue::FixedSizeBinary(*size, value)
        }
        DataType::Struct(_) => min_max_batch_generic(values, Ordering::Less)?,
        DataType::List(_) => min_max_batch_generic(values, Ordering::Less)?,
        DataType::LargeList(_) => min_max_batch_generic(values, Ordering::Less)?,
        DataType::FixedSizeList(_, _) => min_max_batch_generic(values, Ordering::Less)?,
        DataType::Dictionary(_, _) => {
            let values = values.as_any_dictionary().values();
            max_batch(values)?
        }
        _ => min_max_batch!(values, max),
    })
}
