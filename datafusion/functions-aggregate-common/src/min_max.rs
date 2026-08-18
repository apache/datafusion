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
    ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
    Decimal32Array, Decimal64Array, Decimal128Array, Decimal256Array,
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

macro_rules! choose_min_max {
    (min) => {
        std::cmp::Ordering::Greater
    };
    (max) => {
        std::cmp::Ordering::Less
    };
}

macro_rules! min_max {
    ($VALUE:expr, $DELTA:expr, $OP:ident) => {{ min_max_scalar($VALUE, $DELTA, choose_min_max!($OP)) }};
}

fn min_max_option<T: Clone + Ord>(
    lhs: &Option<T>,
    rhs: &Option<T>,
    ordering: Ordering,
) -> Option<T> {
    match (lhs, rhs) {
        (None, None) => None,
        (Some(a), None) => Some(a.clone()),
        (None, Some(b)) => Some(b.clone()),
        (Some(a), Some(b)) if a.cmp(b) == ordering => Some(b.clone()),
        (Some(a), Some(_)) => Some(a.clone()),
    }
}

fn min_max_float_option<T: Copy>(
    lhs: &Option<T>,
    rhs: &Option<T>,
    ordering: Ordering,
    cmp: impl Fn(&T, &T) -> Ordering,
) -> Option<T> {
    match (lhs, rhs) {
        (None, None) => None,
        (Some(a), None) => Some(*a),
        (None, Some(b)) => Some(*b),
        (Some(a), Some(b)) if cmp(a, b) == ordering => Some(*b),
        (Some(a), Some(_)) => Some(*a),
    }
}

fn ensure_decimal_compatibility(
    lhs: &ScalarValue,
    rhs: &ScalarValue,
    lhs_type: (u8, i8),
    rhs_type: (u8, i8),
) -> Result<()> {
    if lhs_type == rhs_type {
        Ok(())
    } else {
        internal_err!(
            "MIN/MAX is not expected to receive scalars of incompatible types {:?}",
            (lhs, rhs)
        )
    }
}

fn min_max_generic_scalar(
    lhs: &ScalarValue,
    rhs: &ScalarValue,
    ordering: Ordering,
) -> ScalarValue {
    if lhs.is_null() {
        let mut rhs_copy = rhs.clone();
        // When the new value won we want to compact it to
        // avoid storing the entire input
        rhs_copy.compact();
        rhs_copy
    } else if rhs.is_null() {
        lhs.clone()
    } else {
        match lhs.partial_cmp(rhs) {
            Some(order) if order == ordering => {
                // When the new value won we want to compact it to
                // avoid storing the entire input
                let mut rhs_copy = rhs.clone();
                rhs_copy.compact();
                rhs_copy
            }
            _ => lhs.clone(),
        }
    }
}

fn min_max_interval_scalar(
    lhs: &ScalarValue,
    rhs: &ScalarValue,
    ordering: Ordering,
) -> Result<ScalarValue> {
    match lhs.partial_cmp(rhs) {
        Some(order) if order == ordering => Ok(rhs.clone()),
        Some(_) => Ok(lhs.clone()),
        None => internal_err!("Comparison error while computing interval min/max"),
    }
}

fn min_max_dictionary_scalar(
    lhs: &ScalarValue,
    rhs: &ScalarValue,
    ordering: Ordering,
) -> Result<Option<ScalarValue>> {
    match (lhs, rhs) {
        (
            ScalarValue::Dictionary(lhs_dict_key_type, lhs_dict_value),
            ScalarValue::Dictionary(rhs_dict_key_type, rhs_dict_value),
        ) => {
            if lhs_dict_key_type != rhs_dict_key_type {
                return internal_err!(
                    "MIN/MAX is not expected to receive dictionary scalars with different key types ({:?} vs {:?})",
                    lhs_dict_key_type,
                    rhs_dict_key_type
                );
            }

            let result = min_max_scalar(
                lhs_dict_value.as_ref(),
                rhs_dict_value.as_ref(),
                ordering,
            )?;
            Ok(Some(ScalarValue::Dictionary(
                lhs_dict_key_type.clone(),
                Box::new(result),
            )))
        }
        (ScalarValue::Dictionary(_, lhs_dict_value), rhs_scalar) => {
            min_max_scalar(lhs_dict_value.as_ref(), rhs_scalar, ordering).map(Some)
        }
        (lhs_scalar, ScalarValue::Dictionary(_, rhs_dict_value)) => {
            min_max_scalar(lhs_scalar, rhs_dict_value.as_ref(), ordering).map(Some)
        }
        _ => Ok(None),
    }
}

// min/max of two logically compatible scalar values.
// Dictionary scalars participate by comparing their inner logical values.
// When both inputs are dictionaries, matching key types are preserved in the
// result; differing key types remain an unexpected invariant violation.
fn min_max_scalar(
    lhs: &ScalarValue,
    rhs: &ScalarValue,
    ordering: Ordering,
) -> Result<ScalarValue> {
    if ordering == Ordering::Equal {
        unreachable!("min/max comparisons do not use equal ordering");
    }

    if let Some(result) = min_max_dictionary_scalar(lhs, rhs, ordering)? {
        return Ok(result);
    }

    min_max_scalar_same_variant(lhs, rhs, ordering)
}

fn min_max_scalar_same_variant(
    lhs: &ScalarValue,
    rhs: &ScalarValue,
    ordering: Ordering,
) -> Result<ScalarValue> {
    let result = match (lhs, rhs) {
        (ScalarValue::Null, ScalarValue::Null) => ScalarValue::Null,
        (
            ScalarValue::Decimal32(lhsv, lhsp, lhss),
            ScalarValue::Decimal32(rhsv, rhsp, rhss),
        ) => {
            ensure_decimal_compatibility(lhs, rhs, (*lhsp, *lhss), (*rhsp, *rhss))?;
            ScalarValue::Decimal32(min_max_option(lhsv, rhsv, ordering), *lhsp, *lhss)
        }
        (
            ScalarValue::Decimal64(lhsv, lhsp, lhss),
            ScalarValue::Decimal64(rhsv, rhsp, rhss),
        ) => {
            ensure_decimal_compatibility(lhs, rhs, (*lhsp, *lhss), (*rhsp, *rhss))?;
            ScalarValue::Decimal64(min_max_option(lhsv, rhsv, ordering), *lhsp, *lhss)
        }
        (
            ScalarValue::Decimal128(lhsv, lhsp, lhss),
            ScalarValue::Decimal128(rhsv, rhsp, rhss),
        ) => {
            ensure_decimal_compatibility(lhs, rhs, (*lhsp, *lhss), (*rhsp, *rhss))?;
            ScalarValue::Decimal128(min_max_option(lhsv, rhsv, ordering), *lhsp, *lhss)
        }
        (
            ScalarValue::Decimal256(lhsv, lhsp, lhss),
            ScalarValue::Decimal256(rhsv, rhsp, rhss),
        ) => {
            ensure_decimal_compatibility(lhs, rhs, (*lhsp, *lhss), (*rhsp, *rhss))?;
            ScalarValue::Decimal256(min_max_option(lhsv, rhsv, ordering), *lhsp, *lhss)
        }
        (ScalarValue::Boolean(lhs), ScalarValue::Boolean(rhs)) => {
            ScalarValue::Boolean(min_max_option(lhs, rhs, ordering))
        }
        (ScalarValue::Float64(lhs), ScalarValue::Float64(rhs)) => {
            ScalarValue::Float64(min_max_float_option(lhs, rhs, ordering, f64::total_cmp))
        }
        (ScalarValue::Float32(lhs), ScalarValue::Float32(rhs)) => {
            ScalarValue::Float32(min_max_float_option(lhs, rhs, ordering, f32::total_cmp))
        }
        (ScalarValue::Float16(lhs), ScalarValue::Float16(rhs)) => {
            ScalarValue::Float16(min_max_float_option(lhs, rhs, ordering, |a, b| {
                a.total_cmp(b)
            }))
        }
        (ScalarValue::UInt64(lhs), ScalarValue::UInt64(rhs)) => {
            ScalarValue::UInt64(min_max_option(lhs, rhs, ordering))
        }
        (ScalarValue::UInt32(lhs), ScalarValue::UInt32(rhs)) => {
            ScalarValue::UInt32(min_max_option(lhs, rhs, ordering))
        }
        (ScalarValue::UInt16(lhs), ScalarValue::UInt16(rhs)) => {
            ScalarValue::UInt16(min_max_option(lhs, rhs, ordering))
        }
        (ScalarValue::UInt8(lhs), ScalarValue::UInt8(rhs)) => {
            ScalarValue::UInt8(min_max_option(lhs, rhs, ordering))
        }
        (ScalarValue::Int64(lhs), ScalarValue::Int64(rhs)) => {
            ScalarValue::Int64(min_max_option(lhs, rhs, ordering))
        }
        (ScalarValue::Int32(lhs), ScalarValue::Int32(rhs)) => {
            ScalarValue::Int32(min_max_option(lhs, rhs, ordering))
        }
        (ScalarValue::Int16(lhs), ScalarValue::Int16(rhs)) => {
            ScalarValue::Int16(min_max_option(lhs, rhs, ordering))
        }
        (ScalarValue::Int8(lhs), ScalarValue::Int8(rhs)) => {
            ScalarValue::Int8(min_max_option(lhs, rhs, ordering))
        }
        (ScalarValue::Utf8(lhs), ScalarValue::Utf8(rhs)) => {
            ScalarValue::Utf8(min_max_option(lhs, rhs, ordering))
        }
        (ScalarValue::LargeUtf8(lhs), ScalarValue::LargeUtf8(rhs)) => {
            ScalarValue::LargeUtf8(min_max_option(lhs, rhs, ordering))
        }
        (ScalarValue::Utf8View(lhs), ScalarValue::Utf8View(rhs)) => {
            ScalarValue::Utf8View(min_max_option(lhs, rhs, ordering))
        }
        (ScalarValue::Binary(lhs), ScalarValue::Binary(rhs)) => {
            ScalarValue::Binary(min_max_option(lhs, rhs, ordering))
        }
        (ScalarValue::LargeBinary(lhs), ScalarValue::LargeBinary(rhs)) => {
            ScalarValue::LargeBinary(min_max_option(lhs, rhs, ordering))
        }
        (
            ScalarValue::FixedSizeBinary(lsize, lhs),
            ScalarValue::FixedSizeBinary(rsize, rhs),
        ) => {
            if lsize == rsize {
                ScalarValue::FixedSizeBinary(*lsize, min_max_option(lhs, rhs, ordering))
            } else {
                return internal_err!(
                    "MIN/MAX is not expected to receive FixedSizeBinary of incompatible sizes {:?}",
                    (lsize, rsize)
                );
            }
        }
        (ScalarValue::BinaryView(lhs), ScalarValue::BinaryView(rhs)) => {
            ScalarValue::BinaryView(min_max_option(lhs, rhs, ordering))
        }
        (
            ScalarValue::TimestampSecond(lhs, l_tz),
            ScalarValue::TimestampSecond(rhs, _),
        ) => {
            ScalarValue::TimestampSecond(min_max_option(lhs, rhs, ordering), l_tz.clone())
        }
        (
            ScalarValue::TimestampMillisecond(lhs, l_tz),
            ScalarValue::TimestampMillisecond(rhs, _),
        ) => ScalarValue::TimestampMillisecond(
            min_max_option(lhs, rhs, ordering),
            l_tz.clone(),
        ),
        (
            ScalarValue::TimestampMicrosecond(lhs, l_tz),
            ScalarValue::TimestampMicrosecond(rhs, _),
        ) => ScalarValue::TimestampMicrosecond(
            min_max_option(lhs, rhs, ordering),
            l_tz.clone(),
        ),
        (
            ScalarValue::TimestampNanosecond(lhs, l_tz),
            ScalarValue::TimestampNanosecond(rhs, _),
        ) => ScalarValue::TimestampNanosecond(
            min_max_option(lhs, rhs, ordering),
            l_tz.clone(),
        ),
        (ScalarValue::Date32(lhs), ScalarValue::Date32(rhs)) => {
            ScalarValue::Date32(min_max_option(lhs, rhs, ordering))
        }
        (ScalarValue::Date64(lhs), ScalarValue::Date64(rhs)) => {
            ScalarValue::Date64(min_max_option(lhs, rhs, ordering))
        }
        (ScalarValue::Time32Second(lhs), ScalarValue::Time32Second(rhs)) => {
            ScalarValue::Time32Second(min_max_option(lhs, rhs, ordering))
        }
        (ScalarValue::Time32Millisecond(lhs), ScalarValue::Time32Millisecond(rhs)) => {
            ScalarValue::Time32Millisecond(min_max_option(lhs, rhs, ordering))
        }
        (ScalarValue::Time64Microsecond(lhs), ScalarValue::Time64Microsecond(rhs)) => {
            ScalarValue::Time64Microsecond(min_max_option(lhs, rhs, ordering))
        }
        (ScalarValue::Time64Nanosecond(lhs), ScalarValue::Time64Nanosecond(rhs)) => {
            ScalarValue::Time64Nanosecond(min_max_option(lhs, rhs, ordering))
        }
        (ScalarValue::IntervalYearMonth(lhs), ScalarValue::IntervalYearMonth(rhs)) => {
            ScalarValue::IntervalYearMonth(min_max_option(lhs, rhs, ordering))
        }
        (
            ScalarValue::IntervalMonthDayNano(lhs),
            ScalarValue::IntervalMonthDayNano(rhs),
        ) => ScalarValue::IntervalMonthDayNano(min_max_option(lhs, rhs, ordering)),
        (ScalarValue::IntervalDayTime(lhs), ScalarValue::IntervalDayTime(rhs)) => {
            ScalarValue::IntervalDayTime(min_max_option(lhs, rhs, ordering))
        }
        (ScalarValue::IntervalYearMonth(_), ScalarValue::IntervalMonthDayNano(_))
        | (ScalarValue::IntervalYearMonth(_), ScalarValue::IntervalDayTime(_))
        | (ScalarValue::IntervalMonthDayNano(_), ScalarValue::IntervalDayTime(_))
        | (ScalarValue::IntervalMonthDayNano(_), ScalarValue::IntervalYearMonth(_))
        | (ScalarValue::IntervalDayTime(_), ScalarValue::IntervalYearMonth(_))
        | (ScalarValue::IntervalDayTime(_), ScalarValue::IntervalMonthDayNano(_)) => {
            return min_max_interval_scalar(lhs, rhs, ordering);
        }
        (ScalarValue::DurationSecond(lhs), ScalarValue::DurationSecond(rhs)) => {
            ScalarValue::DurationSecond(min_max_option(lhs, rhs, ordering))
        }
        (
            ScalarValue::DurationMillisecond(lhs),
            ScalarValue::DurationMillisecond(rhs),
        ) => ScalarValue::DurationMillisecond(min_max_option(lhs, rhs, ordering)),
        (
            ScalarValue::DurationMicrosecond(lhs),
            ScalarValue::DurationMicrosecond(rhs),
        ) => ScalarValue::DurationMicrosecond(min_max_option(lhs, rhs, ordering)),
        (ScalarValue::DurationNanosecond(lhs), ScalarValue::DurationNanosecond(rhs)) => {
            ScalarValue::DurationNanosecond(min_max_option(lhs, rhs, ordering))
        }
        (ScalarValue::Struct(_), ScalarValue::Struct(_))
        | (ScalarValue::List(_), ScalarValue::List(_))
        | (ScalarValue::LargeList(_), ScalarValue::LargeList(_))
        | (ScalarValue::FixedSizeList(_), ScalarValue::FixedSizeList(_)) => {
            min_max_generic_scalar(lhs, rhs, ordering)
        }
        _ => {
            return internal_err!(
                "MIN/MAX is not expected to receive logically incompatible scalar values {:?}",
                (lhs, rhs)
            );
        }
    };

    Ok(result)
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
        DataType::Struct(_)
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Dictionary(_, _) => min_max_batch_generic(values, Ordering::Greater)?,
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
        DataType::Struct(_)
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Dictionary(_, _) => min_max_batch_generic(values, Ordering::Less)?,
        _ => min_max_batch!(values, max),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_max_scalar_preserves_core_behaviors() -> Result<()> {
        let cases = [
            (
                ScalarValue::Int32(Some(1)),
                ScalarValue::Int32(Some(2)),
                Ordering::Less,
                ScalarValue::Int32(Some(2)),
            ),
            (
                ScalarValue::Int32(Some(1)),
                ScalarValue::Int32(Some(2)),
                Ordering::Greater,
                ScalarValue::Int32(Some(1)),
            ),
            (
                ScalarValue::Utf8(Some("a".to_string())),
                ScalarValue::Utf8(Some("b".to_string())),
                Ordering::Less,
                ScalarValue::Utf8(Some("b".to_string())),
            ),
            (
                ScalarValue::Boolean(None),
                ScalarValue::Boolean(Some(true)),
                Ordering::Greater,
                ScalarValue::Boolean(Some(true)),
            ),
        ];

        for (lhs, rhs, ordering, expected) in cases {
            assert_eq!(min_max_scalar(&lhs, &rhs, ordering)?, expected);
        }

        Ok(())
    }

    #[test]
    fn min_max_scalar_float_uses_total_cmp_for_nan() -> Result<()> {
        type F16 =
            <arrow::datatypes::Float16Type as arrow::datatypes::ArrowPrimitiveType>::Native;

        let lhs = ScalarValue::Float64(Some(f64::NAN));
        let rhs = ScalarValue::Float64(Some(1.0));
        assert_eq!(min_max_scalar(&lhs, &rhs, Ordering::Greater)?, rhs);
        assert!(matches!(
            min_max_scalar(&lhs, &rhs, Ordering::Less)?,
            ScalarValue::Float64(Some(value)) if value.is_nan()
        ));

        let lhs = ScalarValue::Float32(Some(f32::NAN));
        let rhs = ScalarValue::Float32(Some(1.0));
        assert_eq!(min_max_scalar(&lhs, &rhs, Ordering::Greater)?, rhs);
        assert!(matches!(
            min_max_scalar(&lhs, &rhs, Ordering::Less)?,
            ScalarValue::Float32(Some(value)) if value.is_nan()
        ));

        let lhs = ScalarValue::Float16(Some(F16::NAN));
        let rhs = ScalarValue::Float16(Some(F16::from_f32(1.0)));
        assert_eq!(min_max_scalar(&lhs, &rhs, Ordering::Greater)?, rhs);
        assert!(matches!(
            min_max_scalar(&lhs, &rhs, Ordering::Less)?,
            ScalarValue::Float16(Some(value)) if value.is_nan()
        ));
        Ok(())
    }

    #[test]
    fn min_max_decimal_mismatch_error_is_preserved() -> Result<()> {
        let lhs = ScalarValue::Decimal128(Some(1), 10, 2);
        let rhs = ScalarValue::Decimal128(Some(2), 11, 2);

        let error = min_max_scalar(&lhs, &rhs, Ordering::Less).unwrap_err();
        let message = error.to_string();

        assert!(message.starts_with(&format!(
            "Internal error: MIN/MAX is not expected to receive scalars of incompatible types {:?}",
            (&lhs, &rhs)
        )));
        Ok(())
    }

    #[test]
    fn min_max_fixed_size_binary_mismatch_error_is_preserved() -> Result<()> {
        let lhs = ScalarValue::FixedSizeBinary(2, Some(vec![1, 2]));
        let rhs = ScalarValue::FixedSizeBinary(3, Some(vec![1, 2, 3]));

        let error = min_max_scalar(&lhs, &rhs, Ordering::Less).unwrap_err();
        let message = error.to_string();

        assert!(message.starts_with(
            "Internal error: MIN/MAX is not expected to receive FixedSizeBinary of incompatible sizes (2, 3)"
        ));
        Ok(())
    }

    #[test]
    fn min_max_mixed_interval_error_is_preserved() -> Result<()> {
        let lhs = ScalarValue::IntervalYearMonth(Some(1));
        let rhs = ScalarValue::IntervalDayTime(Some(
            arrow::datatypes::IntervalDayTime::new(1, 0),
        ));

        let error = min_max_scalar(&lhs, &rhs, Ordering::Less).unwrap_err();
        let message = error.to_string();

        assert!(message.starts_with(
            "Internal error: Comparison error while computing interval min/max"
        ));
        Ok(())
    }

    #[test]
    fn min_max_dictionary_and_scalar_compare_by_inner_value() -> Result<()> {
        let dictionary = ScalarValue::Dictionary(
            Box::new(DataType::Int32),
            Box::new(ScalarValue::Float32(Some(1.0))),
        );
        let scalar = ScalarValue::Float32(Some(2.0));

        let result = min_max_scalar(&dictionary, &scalar, Ordering::Less)?;

        assert_eq!(result, ScalarValue::Float32(Some(2.0)));
        Ok(())
    }

    #[test]
    fn min_max_dictionary_same_key_type_rewraps_result() -> Result<()> {
        let lhs = ScalarValue::Dictionary(
            Box::new(DataType::Int32),
            Box::new(ScalarValue::Float32(Some(1.0))),
        );
        let rhs = ScalarValue::Dictionary(
            Box::new(DataType::Int32),
            Box::new(ScalarValue::Float32(Some(2.0))),
        );

        let result = min_max_scalar(&lhs, &rhs, Ordering::Less)?;

        assert_eq!(
            result,
            ScalarValue::Dictionary(
                Box::new(DataType::Int32),
                Box::new(ScalarValue::Float32(Some(2.0))),
            )
        );
        Ok(())
    }

    #[test]
    fn min_max_dictionary_different_key_types_error() -> Result<()> {
        let lhs = ScalarValue::Dictionary(
            Box::new(DataType::Int8),
            Box::new(ScalarValue::Float32(Some(1.0))),
        );
        let rhs = ScalarValue::Dictionary(
            Box::new(DataType::Int32),
            Box::new(ScalarValue::Float32(Some(2.0))),
        );

        let error: DataFusionError =
            min_max_scalar(&lhs, &rhs, Ordering::Less).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("dictionary scalars with different key types")
        );
        Ok(())
    }

    #[test]
    fn min_max_dictionary_and_incompatible_scalar_error() -> Result<()> {
        let dictionary = ScalarValue::Dictionary(
            Box::new(DataType::Int32),
            Box::new(ScalarValue::Float32(Some(1.0))),
        );
        let scalar = ScalarValue::Int32(Some(2));

        let error: DataFusionError =
            min_max_scalar(&dictionary, &scalar, Ordering::Less).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("logically incompatible scalar values")
        );
        Ok(())
    }
}
