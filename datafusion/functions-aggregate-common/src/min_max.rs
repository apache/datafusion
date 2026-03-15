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
    Date64Array, Decimal32Array, Decimal64Array, Decimal128Array, Decimal256Array,
    DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
    DurationSecondArray, FixedSizeBinaryArray, Float16Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, IntervalDayTimeArray,
    IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeBinaryArray,
    LargeStringArray, StringArray, StringViewArray, Time32MillisecondArray,
    Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::compute;
use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion_common::{
    DataFusionError, Result, ScalarValue, downcast_value, internal_err,
};
use datafusion_expr_common::accumulator::Accumulator;
use std::{cmp::Ordering, mem::size_of_val};

// min/max of two non-string scalar values.
macro_rules! typed_min_max {
    ($VALUE:expr, $DELTA:expr, $SCALAR:ident, $OP:ident $(, $EXTRA_ARGS:ident)*) => {{
        ScalarValue::$SCALAR(
            match ($VALUE, $DELTA) {
                (None, None) => None,
                (Some(a), None) => Some(*a),
                (None, Some(b)) => Some(*b),
                (Some(a), Some(b)) => Some((*a).$OP(*b)),
            },
            $($EXTRA_ARGS.clone()),*
        )
    }};
}

macro_rules! typed_min_max_float {
    ($VALUE:expr, $DELTA:expr, $SCALAR:ident, $OP:ident) => {{
        ScalarValue::$SCALAR(match ($VALUE, $DELTA) {
            (None, None) => None,
            (Some(a), None) => Some(*a),
            (None, Some(b)) => Some(*b),
            (Some(a), Some(b)) => match a.total_cmp(b) {
                choose_min_max!($OP) => Some(*b),
                _ => Some(*a),
            },
        })
    }};
}

// min/max of two scalar string values.
macro_rules! typed_min_max_string {
    ($VALUE:expr, $DELTA:expr, $SCALAR:ident, $OP:ident) => {{
        ScalarValue::$SCALAR(match ($VALUE, $DELTA) {
            (None, None) => None,
            (Some(a), None) => Some(a.clone()),
            (None, Some(b)) => Some(b.clone()),
            (Some(a), Some(b)) => Some((a).$OP(b).clone()),
        })
    }};
}

// min/max of two scalar string values with a prefix argument.
macro_rules! typed_min_max_string_arg {
    ($VALUE:expr, $DELTA:expr, $SCALAR:ident, $OP:ident, $ARG:expr) => {{
        ScalarValue::$SCALAR(
            $ARG,
            match ($VALUE, $DELTA) {
                (None, None) => None,
                (Some(a), None) => Some(a.clone()),
                (None, Some(b)) => Some(b.clone()),
                (Some(a), Some(b)) => Some((a).$OP(b).clone()),
            },
        )
    }};
}

macro_rules! choose_min_max {
    (min) => {
        std::cmp::Ordering::Greater
    };
    (max) => {
        std::cmp::Ordering::Less
    };
}

macro_rules! interval_min_max {
    ($OP:tt, $LHS:expr, $RHS:expr) => {{
        match $LHS.partial_cmp(&$RHS) {
            Some(choose_min_max!($OP)) => $RHS.clone(),
            Some(_) => $LHS.clone(),
            None => {
                return internal_err!(
                    "Comparison error while computing interval min/max"
                );
            }
        }
    }};
}

macro_rules! min_max_generic {
    ($VALUE:expr, $DELTA:expr, $OP:ident) => {{
        if $VALUE.is_null() {
            let mut delta_copy = $DELTA.clone();
            // When the new value won we want to compact it to
            // avoid storing the entire input
            delta_copy.compact();
            delta_copy
        } else if $DELTA.is_null() {
            $VALUE.clone()
        } else {
            match $VALUE.partial_cmp(&$DELTA) {
                Some(choose_min_max!($OP)) => {
                    // When the new value won we want to compact it to
                    // avoid storing the entire input
                    let mut delta_copy = $DELTA.clone();
                    delta_copy.compact();
                    delta_copy
                }
                _ => $VALUE.clone(),
            }
        }
    }};
}

// min/max of two scalar values of the same type
macro_rules! min_max {
    ($VALUE:expr, $DELTA:expr, $OP:ident) => {{
        Ok(match ($VALUE, $DELTA) {
            (ScalarValue::Null, ScalarValue::Null) => ScalarValue::Null,
            (
                lhs @ ScalarValue::Decimal32(lhsv, lhsp, lhss),
                rhs @ ScalarValue::Decimal32(rhsv, rhsp, rhss)
            ) => {
                if lhsp.eq(rhsp) && lhss.eq(rhss) {
                    typed_min_max!(lhsv, rhsv, Decimal32, $OP, lhsp, lhss)
                } else {
                    return internal_err!(
                    "MIN/MAX is not expected to receive scalars of incompatible types {:?}",
                    (lhs, rhs)
                );
                }
            }
            (
                lhs @ ScalarValue::Decimal64(lhsv, lhsp, lhss),
                rhs @ ScalarValue::Decimal64(rhsv, rhsp, rhss)
            ) => {
                if lhsp.eq(rhsp) && lhss.eq(rhss) {
                    typed_min_max!(lhsv, rhsv, Decimal64, $OP, lhsp, lhss)
                } else {
                    return internal_err!(
                    "MIN/MAX is not expected to receive scalars of incompatible types {:?}",
                    (lhs, rhs)
                );
                }
            }
            (
                lhs @ ScalarValue::Decimal128(lhsv, lhsp, lhss),
                rhs @ ScalarValue::Decimal128(rhsv, rhsp, rhss)
            ) => {
                if lhsp.eq(rhsp) && lhss.eq(rhss) {
                    typed_min_max!(lhsv, rhsv, Decimal128, $OP, lhsp, lhss)
                } else {
                    return internal_err!(
                    "MIN/MAX is not expected to receive scalars of incompatible types {:?}",
                    (lhs, rhs)
                );
                }
            }
            (
                lhs @ ScalarValue::Decimal256(lhsv, lhsp, lhss),
                rhs @ ScalarValue::Decimal256(rhsv, rhsp, rhss)
            ) => {
                if lhsp.eq(rhsp) && lhss.eq(rhss) {
                    typed_min_max!(lhsv, rhsv, Decimal256, $OP, lhsp, lhss)
                } else {
                    return internal_err!(
                    "MIN/MAX is not expected to receive scalars of incompatible types {:?}",
                    (lhs, rhs)
                );
                }
            }
            (ScalarValue::Boolean(lhs), ScalarValue::Boolean(rhs)) => {
                typed_min_max!(lhs, rhs, Boolean, $OP)
            }
            (ScalarValue::Float64(lhs), ScalarValue::Float64(rhs)) => {
                typed_min_max_float!(lhs, rhs, Float64, $OP)
            }
            (ScalarValue::Float32(lhs), ScalarValue::Float32(rhs)) => {
                typed_min_max_float!(lhs, rhs, Float32, $OP)
            }
            (ScalarValue::Float16(lhs), ScalarValue::Float16(rhs)) => {
                typed_min_max_float!(lhs, rhs, Float16, $OP)
            }
            (ScalarValue::UInt64(lhs), ScalarValue::UInt64(rhs)) => {
                typed_min_max!(lhs, rhs, UInt64, $OP)
            }
            (ScalarValue::UInt32(lhs), ScalarValue::UInt32(rhs)) => {
                typed_min_max!(lhs, rhs, UInt32, $OP)
            }
            (ScalarValue::UInt16(lhs), ScalarValue::UInt16(rhs)) => {
                typed_min_max!(lhs, rhs, UInt16, $OP)
            }
            (ScalarValue::UInt8(lhs), ScalarValue::UInt8(rhs)) => {
                typed_min_max!(lhs, rhs, UInt8, $OP)
            }
            (ScalarValue::Int64(lhs), ScalarValue::Int64(rhs)) => {
                typed_min_max!(lhs, rhs, Int64, $OP)
            }
            (ScalarValue::Int32(lhs), ScalarValue::Int32(rhs)) => {
                typed_min_max!(lhs, rhs, Int32, $OP)
            }
            (ScalarValue::Int16(lhs), ScalarValue::Int16(rhs)) => {
                typed_min_max!(lhs, rhs, Int16, $OP)
            }
            (ScalarValue::Int8(lhs), ScalarValue::Int8(rhs)) => {
                typed_min_max!(lhs, rhs, Int8, $OP)
            }
            (ScalarValue::Utf8(lhs), ScalarValue::Utf8(rhs)) => {
                typed_min_max_string!(lhs, rhs, Utf8, $OP)
            }
            (ScalarValue::LargeUtf8(lhs), ScalarValue::LargeUtf8(rhs)) => {
                typed_min_max_string!(lhs, rhs, LargeUtf8, $OP)
            }
            (ScalarValue::Utf8View(lhs), ScalarValue::Utf8View(rhs)) => {
                typed_min_max_string!(lhs, rhs, Utf8View, $OP)
            }
            (ScalarValue::Binary(lhs), ScalarValue::Binary(rhs)) => {
                typed_min_max_string!(lhs, rhs, Binary, $OP)
            }
            (ScalarValue::LargeBinary(lhs), ScalarValue::LargeBinary(rhs)) => {
                typed_min_max_string!(lhs, rhs, LargeBinary, $OP)
            }
            (ScalarValue::FixedSizeBinary(lsize, lhs), ScalarValue::FixedSizeBinary(rsize, rhs)) => {
                if lsize == rsize {
                    typed_min_max_string_arg!(lhs, rhs, FixedSizeBinary, $OP, *lsize)
                }
                else {
                    return internal_err!(
                        "MIN/MAX is not expected to receive FixedSizeBinary of incompatible sizes {:?}",
                        (lsize, rsize))
                }
            }
            (ScalarValue::BinaryView(lhs), ScalarValue::BinaryView(rhs)) => {
                typed_min_max_string!(lhs, rhs, BinaryView, $OP)
            }
            (ScalarValue::TimestampSecond(lhs, l_tz), ScalarValue::TimestampSecond(rhs, _)) => {
                typed_min_max!(lhs, rhs, TimestampSecond, $OP, l_tz)
            }
            (
                ScalarValue::TimestampMillisecond(lhs, l_tz),
                ScalarValue::TimestampMillisecond(rhs, _),
            ) => {
                typed_min_max!(lhs, rhs, TimestampMillisecond, $OP, l_tz)
            }
            (
                ScalarValue::TimestampMicrosecond(lhs, l_tz),
                ScalarValue::TimestampMicrosecond(rhs, _),
            ) => {
                typed_min_max!(lhs, rhs, TimestampMicrosecond, $OP, l_tz)
            }
            (
                ScalarValue::TimestampNanosecond(lhs, l_tz),
                ScalarValue::TimestampNanosecond(rhs, _),
            ) => {
                typed_min_max!(lhs, rhs, TimestampNanosecond, $OP, l_tz)
            }
            (
                ScalarValue::Date32(lhs),
                ScalarValue::Date32(rhs),
            ) => {
                typed_min_max!(lhs, rhs, Date32, $OP)
            }
            (
                ScalarValue::Date64(lhs),
                ScalarValue::Date64(rhs),
            ) => {
                typed_min_max!(lhs, rhs, Date64, $OP)
            }
            (
                ScalarValue::Time32Second(lhs),
                ScalarValue::Time32Second(rhs),
            ) => {
                typed_min_max!(lhs, rhs, Time32Second, $OP)
            }
            (
                ScalarValue::Time32Millisecond(lhs),
                ScalarValue::Time32Millisecond(rhs),
            ) => {
                typed_min_max!(lhs, rhs, Time32Millisecond, $OP)
            }
            (
                ScalarValue::Time64Microsecond(lhs),
                ScalarValue::Time64Microsecond(rhs),
            ) => {
                typed_min_max!(lhs, rhs, Time64Microsecond, $OP)
            }
            (
                ScalarValue::Time64Nanosecond(lhs),
                ScalarValue::Time64Nanosecond(rhs),
            ) => {
                typed_min_max!(lhs, rhs, Time64Nanosecond, $OP)
            }
            (
                ScalarValue::IntervalYearMonth(lhs),
                ScalarValue::IntervalYearMonth(rhs),
            ) => {
                typed_min_max!(lhs, rhs, IntervalYearMonth, $OP)
            }
            (
                ScalarValue::IntervalMonthDayNano(lhs),
                ScalarValue::IntervalMonthDayNano(rhs),
            ) => {
                typed_min_max!(lhs, rhs, IntervalMonthDayNano, $OP)
            }
            (
                ScalarValue::IntervalDayTime(lhs),
                ScalarValue::IntervalDayTime(rhs),
            ) => {
                typed_min_max!(lhs, rhs, IntervalDayTime, $OP)
            }
            (
                ScalarValue::IntervalYearMonth(_),
                ScalarValue::IntervalMonthDayNano(_),
            ) | (
                ScalarValue::IntervalYearMonth(_),
                ScalarValue::IntervalDayTime(_),
            ) | (
                ScalarValue::IntervalMonthDayNano(_),
                ScalarValue::IntervalDayTime(_),
            ) | (
                ScalarValue::IntervalMonthDayNano(_),
                ScalarValue::IntervalYearMonth(_),
            ) | (
                ScalarValue::IntervalDayTime(_),
                ScalarValue::IntervalYearMonth(_),
            ) | (
                ScalarValue::IntervalDayTime(_),
                ScalarValue::IntervalMonthDayNano(_),
            ) => {
                interval_min_max!($OP, $VALUE, $DELTA)
            }
                    (
                ScalarValue::DurationSecond(lhs),
                ScalarValue::DurationSecond(rhs),
            ) => {
                typed_min_max!(lhs, rhs, DurationSecond, $OP)
            }
                                (
                ScalarValue::DurationMillisecond(lhs),
                ScalarValue::DurationMillisecond(rhs),
            ) => {
                typed_min_max!(lhs, rhs, DurationMillisecond, $OP)
            }
                                (
                ScalarValue::DurationMicrosecond(lhs),
                ScalarValue::DurationMicrosecond(rhs),
            ) => {
                typed_min_max!(lhs, rhs, DurationMicrosecond, $OP)
            }
                                        (
                ScalarValue::DurationNanosecond(lhs),
                ScalarValue::DurationNanosecond(rhs),
            ) => {
                typed_min_max!(lhs, rhs, DurationNanosecond, $OP)
            }

            (
                lhs @ ScalarValue::Struct(_),
                rhs @ ScalarValue::Struct(_),
            ) => {
                min_max_generic!(lhs, rhs, $OP)
            }

            (
                lhs @ ScalarValue::List(_),
                rhs @ ScalarValue::List(_),
            ) => {
                min_max_generic!(lhs, rhs, $OP)
            }


            (
                lhs @ ScalarValue::LargeList(_),
                rhs @ ScalarValue::LargeList(_),
            ) => {
                min_max_generic!(lhs, rhs, $OP)
            }


            (
                lhs @ ScalarValue::FixedSizeList(_),
                rhs @ ScalarValue::FixedSizeList(_),
            ) => {
                min_max_generic!(lhs, rhs, $OP)
            }

            e => {
                return internal_err!(
                    "MIN/MAX is not expected to receive scalars of incompatible types {:?}",
                    e
                )
            }
        })
    }};
}

/// An accumulator to compute the maximum value
#[derive(Debug, Clone)]
pub struct MaxAccumulator {
    max: ScalarValue,
}

impl MaxAccumulator {
    /// new max accumulator
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        Ok(Self {
            max: ScalarValue::try_from(datatype)?,
        })
    }
}

impl Accumulator for MaxAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];
        let delta = &max_batch(values)?;
        let new_max: Result<ScalarValue, DataFusionError> =
            min_max!(&self.max, delta, max);
        self.max = new_max?;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.max.clone())
    }

    fn size(&self) -> usize {
        size_of_val(self) - size_of_val(&self.max) + self.max.size()
    }
}

/// An accumulator to compute the minimum value
#[derive(Debug, Clone)]
pub struct MinAccumulator {
    min: ScalarValue,
}

impl MinAccumulator {
    /// new min accumulator
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        Ok(Self {
            min: ScalarValue::try_from(datatype)?,
        })
    }
}

impl Accumulator for MinAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];
        let delta = &min_batch(values)?;
        let new_min: Result<ScalarValue, DataFusionError> =
            min_max!(&self.min, delta, min);
        self.min = new_min?;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.min.clone())
    }

    fn size(&self) -> usize {
        size_of_val(self) - size_of_val(&self.min) + self.min.size()
    }
}

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
            DataType::Decimal32(precision, scale) => {
                typed_min_max_batch!(
                    $VALUES,
                    Decimal32Array,
                    Decimal32,
                    $OP,
                    precision,
                    scale
                )
            }
            DataType::Decimal64(precision, scale) => {
                typed_min_max_batch!(
                    $VALUES,
                    Decimal64Array,
                    Decimal64,
                    $OP,
                    precision,
                    scale
                )
            }
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
                    "Min/Max accumulator not implemented for type {}",
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
