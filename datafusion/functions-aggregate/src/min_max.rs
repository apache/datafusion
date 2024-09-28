// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
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

//! [`Max`] and [`MaxAccumulator`] accumulator for the `max` function
//! [`Min`] and [`MinAccumulator`] accumulator for the `min` function

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

use arrow::array::{
    ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
    Decimal128Array, Decimal256Array, Float16Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, IntervalDayTimeArray,
    IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeBinaryArray,
    LargeStringArray, StringArray, StringViewArray, Time32MillisecondArray,
    Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::compute;
use arrow::datatypes::{
    DataType, Decimal128Type, Decimal256Type, Float16Type, Float32Type, Float64Type,
    Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type,
    UInt8Type,
};
use arrow_schema::IntervalUnit;
use datafusion_common::stats::Precision;
use datafusion_common::{
    downcast_value, exec_err, internal_err, ColumnStatistics, DataFusionError, Result,
};
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::prim_op::PrimitiveGroupsAccumulator;
use datafusion_physical_expr::expressions;
use std::fmt::Debug;

use arrow::datatypes::i256;
use arrow::datatypes::{
    Date32Type, Date64Type, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};

use datafusion_common::ScalarValue;
use datafusion_expr::{
    function::AccumulatorArgs, Accumulator, AggregateUDFImpl, Signature, Volatility,
};
use datafusion_expr::{GroupsAccumulator, StatisticsArgs};
use half::f16;
use std::ops::Deref;

fn get_min_max_result_type(input_types: &[DataType]) -> Result<Vec<DataType>> {
    // make sure that the input types only has one element.
    if input_types.len() != 1 {
        return exec_err!(
            "min/max was called with {} arguments. It requires only 1.",
            input_types.len()
        );
    }
    // min and max support the dictionary data type
    // unpack the dictionary to get the value
    match &input_types[0] {
        DataType::Dictionary(_, dict_value_type) => {
            // TODO add checker, if the value type is complex data type
            Ok(vec![dict_value_type.deref().clone()])
        }
        // TODO add checker for datatype which min and max supported
        // For example, the `Struct` and `Map` type are not supported in the MIN and MAX function
        _ => Ok(input_types.to_vec()),
    }
}

// MAX aggregate UDF
#[derive(Debug)]
pub struct Max {
    signature: Signature,
}

impl Max {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for Max {
    fn default() -> Self {
        Self::new()
    }
}
/// Creates a [`PrimitiveGroupsAccumulator`] for computing `MAX`
/// the specified [`ArrowPrimitiveType`].
///
/// [`ArrowPrimitiveType`]: arrow::datatypes::ArrowPrimitiveType
macro_rules! instantiate_max_accumulator {
    ($DATA_TYPE:ident, $NATIVE:ident, $PRIMTYPE:ident) => {{
        Ok(Box::new(
            PrimitiveGroupsAccumulator::<$PRIMTYPE, _>::new($DATA_TYPE, |cur, new| {
                if *cur < new {
                    *cur = new
                }
            })
            // Initialize each accumulator to $NATIVE::MIN
            .with_starting_value($NATIVE::MIN),
        ))
    }};
}

/// Creates a [`PrimitiveGroupsAccumulator`] for computing `MIN`
/// the specified [`ArrowPrimitiveType`].
///
///
/// [`ArrowPrimitiveType`]: arrow::datatypes::ArrowPrimitiveType
macro_rules! instantiate_min_accumulator {
    ($DATA_TYPE:ident, $NATIVE:ident, $PRIMTYPE:ident) => {{
        Ok(Box::new(
            PrimitiveGroupsAccumulator::<$PRIMTYPE, _>::new(&$DATA_TYPE, |cur, new| {
                if *cur > new {
                    *cur = new
                }
            })
            // Initialize each accumulator to $NATIVE::MAX
            .with_starting_value($NATIVE::MAX),
        ))
    }};
}

trait FromColumnStatistics {
    fn value_from_column_statistics(
        &self,
        stats: &ColumnStatistics,
    ) -> Option<ScalarValue>;

    fn value_from_statistics(
        &self,
        statistics_args: &StatisticsArgs,
    ) -> Option<ScalarValue> {
        if let Precision::Exact(num_rows) = &statistics_args.statistics.num_rows {
            match *num_rows {
                0 => return ScalarValue::try_from(statistics_args.return_type).ok(),
                value if value > 0 => {
                    let col_stats = &statistics_args.statistics.column_statistics;
                    if statistics_args.exprs.len() == 1 {
                        // TODO optimize with exprs other than Column
                        if let Some(col_expr) = statistics_args.exprs[0]
                            .as_any()
                            .downcast_ref::<expressions::Column>()
                        {
                            return self.value_from_column_statistics(
                                &col_stats[col_expr.index()],
                            );
                        }
                    }
                }
                _ => {}
            }
        }
        None
    }
}

impl FromColumnStatistics for Max {
    fn value_from_column_statistics(
        &self,
        col_stats: &ColumnStatistics,
    ) -> Option<ScalarValue> {
        if let Precision::Exact(ref val) = col_stats.max_value {
            if !val.is_null() {
                return Some(val.clone());
            }
        }
        None
    }
}

impl AggregateUDFImpl for Max {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "max"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].to_owned())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(MaxAccumulator::try_new(acc_args.return_type)?))
    }

    fn aliases(&self) -> &[String] {
        &[]
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        use DataType::*;
        matches!(
            args.return_type,
            Int8 | Int16
                | Int32
                | Int64
                | UInt8
                | UInt16
                | UInt32
                | UInt64
                | Float16
                | Float32
                | Float64
                | Decimal128(_, _)
                | Decimal256(_, _)
                | Date32
                | Date64
                | Time32(_)
                | Time64(_)
                | Timestamp(_, _)
        )
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        use DataType::*;
        use TimeUnit::*;
        let data_type = args.return_type;
        match data_type {
            Int8 => instantiate_max_accumulator!(data_type, i8, Int8Type),
            Int16 => instantiate_max_accumulator!(data_type, i16, Int16Type),
            Int32 => instantiate_max_accumulator!(data_type, i32, Int32Type),
            Int64 => instantiate_max_accumulator!(data_type, i64, Int64Type),
            UInt8 => instantiate_max_accumulator!(data_type, u8, UInt8Type),
            UInt16 => instantiate_max_accumulator!(data_type, u16, UInt16Type),
            UInt32 => instantiate_max_accumulator!(data_type, u32, UInt32Type),
            UInt64 => instantiate_max_accumulator!(data_type, u64, UInt64Type),
            Float16 => {
                instantiate_max_accumulator!(data_type, f16, Float16Type)
            }
            Float32 => {
                instantiate_max_accumulator!(data_type, f32, Float32Type)
            }
            Float64 => {
                instantiate_max_accumulator!(data_type, f64, Float64Type)
            }
            Date32 => instantiate_max_accumulator!(data_type, i32, Date32Type),
            Date64 => instantiate_max_accumulator!(data_type, i64, Date64Type),
            Time32(Second) => {
                instantiate_max_accumulator!(data_type, i32, Time32SecondType)
            }
            Time32(Millisecond) => {
                instantiate_max_accumulator!(data_type, i32, Time32MillisecondType)
            }
            Time64(Microsecond) => {
                instantiate_max_accumulator!(data_type, i64, Time64MicrosecondType)
            }
            Time64(Nanosecond) => {
                instantiate_max_accumulator!(data_type, i64, Time64NanosecondType)
            }
            Timestamp(Second, _) => {
                instantiate_max_accumulator!(data_type, i64, TimestampSecondType)
            }
            Timestamp(Millisecond, _) => {
                instantiate_max_accumulator!(data_type, i64, TimestampMillisecondType)
            }
            Timestamp(Microsecond, _) => {
                instantiate_max_accumulator!(data_type, i64, TimestampMicrosecondType)
            }
            Timestamp(Nanosecond, _) => {
                instantiate_max_accumulator!(data_type, i64, TimestampNanosecondType)
            }
            Decimal128(_, _) => {
                instantiate_max_accumulator!(data_type, i128, Decimal128Type)
            }
            Decimal256(_, _) => {
                instantiate_max_accumulator!(data_type, i256, Decimal256Type)
            }

            // It would be nice to have a fast implementation for Strings as well
            // https://github.com/apache/datafusion/issues/6906

            // This is only reached if groups_accumulator_supported is out of sync
            _ => internal_err!("GroupsAccumulator not supported for max({})", data_type),
        }
    }

    fn create_sliding_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SlidingMaxAccumulator::try_new(args.return_type)?))
    }

    fn is_descending(&self) -> Option<bool> {
        Some(true)
    }

    fn order_sensitivity(&self) -> datafusion_expr::utils::AggregateOrderSensitivity {
        datafusion_expr::utils::AggregateOrderSensitivity::Insensitive
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        get_min_max_result_type(arg_types)
    }
    fn reverse_expr(&self) -> datafusion_expr::ReversedUDAF {
        datafusion_expr::ReversedUDAF::Identical
    }
    fn value_from_stats(&self, statistics_args: &StatisticsArgs) -> Option<ScalarValue> {
        self.value_from_statistics(statistics_args)
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
// Statically-typed version of min/max(array) -> ScalarValue for binay types.
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
            other => {
                // This should have been handled before
                return internal_err!(
                    "Min/Max accumulator not implemented for type {:?}",
                    other
                );
            }
        }
    }};
}

/// dynamically-typed min(array) -> ScalarValue
fn min_batch(values: &ArrayRef) -> Result<ScalarValue> {
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
        DataType::BinaryView => {
            typed_min_max_batch_binary!(
                &values,
                BinaryViewArray,
                BinaryView,
                min_binary_view
            )
        }
        _ => min_max_batch!(values, min),
    })
}

/// dynamically-typed max(array) -> ScalarValue
fn max_batch(values: &ArrayRef) -> Result<ScalarValue> {
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
        _ => min_max_batch!(values, max),
    })
}

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
                return internal_err!("Comparison error while computing interval min/max")
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
#[derive(Debug)]
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
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.max) + self.max.size()
    }
}

#[derive(Debug)]
pub struct SlidingMaxAccumulator {
    max: ScalarValue,
    moving_max: MovingMax<ScalarValue>,
}

impl SlidingMaxAccumulator {
    /// new max accumulator
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        Ok(Self {
            max: ScalarValue::try_from(datatype)?,
            moving_max: MovingMax::<ScalarValue>::new(),
        })
    }
}

impl Accumulator for SlidingMaxAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for idx in 0..values[0].len() {
            let val = ScalarValue::try_from_array(&values[0], idx)?;
            self.moving_max.push(val);
        }
        if let Some(res) = self.moving_max.max() {
            self.max = res.clone();
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for _idx in 0..values[0].len() {
            (self.moving_max).pop();
        }
        if let Some(res) = self.moving_max.max() {
            self.max = res.clone();
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.max.clone()])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.max.clone())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.max) + self.max.size()
    }
}

#[derive(Debug)]
pub struct Min {
    signature: Signature,
}

impl Min {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for Min {
    fn default() -> Self {
        Self::new()
    }
}

impl FromColumnStatistics for Min {
    fn value_from_column_statistics(
        &self,
        col_stats: &ColumnStatistics,
    ) -> Option<ScalarValue> {
        if let Precision::Exact(ref val) = col_stats.min_value {
            if !val.is_null() {
                return Some(val.clone());
            }
        }
        None
    }
}

impl AggregateUDFImpl for Min {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "min"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].to_owned())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(MinAccumulator::try_new(acc_args.return_type)?))
    }

    fn aliases(&self) -> &[String] {
        &[]
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        use DataType::*;
        matches!(
            args.return_type,
            Int8 | Int16
                | Int32
                | Int64
                | UInt8
                | UInt16
                | UInt32
                | UInt64
                | Float16
                | Float32
                | Float64
                | Decimal128(_, _)
                | Decimal256(_, _)
                | Date32
                | Date64
                | Time32(_)
                | Time64(_)
                | Timestamp(_, _)
        )
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        use DataType::*;
        use TimeUnit::*;
        let data_type = args.return_type;
        match data_type {
            Int8 => instantiate_min_accumulator!(data_type, i8, Int8Type),
            Int16 => instantiate_min_accumulator!(data_type, i16, Int16Type),
            Int32 => instantiate_min_accumulator!(data_type, i32, Int32Type),
            Int64 => instantiate_min_accumulator!(data_type, i64, Int64Type),
            UInt8 => instantiate_min_accumulator!(data_type, u8, UInt8Type),
            UInt16 => instantiate_min_accumulator!(data_type, u16, UInt16Type),
            UInt32 => instantiate_min_accumulator!(data_type, u32, UInt32Type),
            UInt64 => instantiate_min_accumulator!(data_type, u64, UInt64Type),
            Float16 => {
                instantiate_min_accumulator!(data_type, f16, Float16Type)
            }
            Float32 => {
                instantiate_min_accumulator!(data_type, f32, Float32Type)
            }
            Float64 => {
                instantiate_min_accumulator!(data_type, f64, Float64Type)
            }
            Date32 => instantiate_min_accumulator!(data_type, i32, Date32Type),
            Date64 => instantiate_min_accumulator!(data_type, i64, Date64Type),
            Time32(Second) => {
                instantiate_min_accumulator!(data_type, i32, Time32SecondType)
            }
            Time32(Millisecond) => {
                instantiate_min_accumulator!(data_type, i32, Time32MillisecondType)
            }
            Time64(Microsecond) => {
                instantiate_min_accumulator!(data_type, i64, Time64MicrosecondType)
            }
            Time64(Nanosecond) => {
                instantiate_min_accumulator!(data_type, i64, Time64NanosecondType)
            }
            Timestamp(Second, _) => {
                instantiate_min_accumulator!(data_type, i64, TimestampSecondType)
            }
            Timestamp(Millisecond, _) => {
                instantiate_min_accumulator!(data_type, i64, TimestampMillisecondType)
            }
            Timestamp(Microsecond, _) => {
                instantiate_min_accumulator!(data_type, i64, TimestampMicrosecondType)
            }
            Timestamp(Nanosecond, _) => {
                instantiate_min_accumulator!(data_type, i64, TimestampNanosecondType)
            }
            Decimal128(_, _) => {
                instantiate_min_accumulator!(data_type, i128, Decimal128Type)
            }
            Decimal256(_, _) => {
                instantiate_min_accumulator!(data_type, i256, Decimal256Type)
            }

            // It would be nice to have a fast implementation for Strings as well
            // https://github.com/apache/datafusion/issues/6906

            // This is only reached if groups_accumulator_supported is out of sync
            _ => internal_err!("GroupsAccumulator not supported for min({})", data_type),
        }
    }

    fn create_sliding_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SlidingMinAccumulator::try_new(args.return_type)?))
    }

    fn is_descending(&self) -> Option<bool> {
        Some(false)
    }

    fn value_from_stats(&self, statistics_args: &StatisticsArgs) -> Option<ScalarValue> {
        self.value_from_statistics(statistics_args)
    }
    fn order_sensitivity(&self) -> datafusion_expr::utils::AggregateOrderSensitivity {
        datafusion_expr::utils::AggregateOrderSensitivity::Insensitive
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        get_min_max_result_type(arg_types)
    }

    fn reverse_expr(&self) -> datafusion_expr::ReversedUDAF {
        datafusion_expr::ReversedUDAF::Identical
    }
}
/// An accumulator to compute the minimum value
#[derive(Debug)]
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
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.min) + self.min.size()
    }
}

#[derive(Debug)]
pub struct SlidingMinAccumulator {
    min: ScalarValue,
    moving_min: MovingMin<ScalarValue>,
}

impl SlidingMinAccumulator {
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        Ok(Self {
            min: ScalarValue::try_from(datatype)?,
            moving_min: MovingMin::<ScalarValue>::new(),
        })
    }
}

impl Accumulator for SlidingMinAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.min.clone()])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for idx in 0..values[0].len() {
            let val = ScalarValue::try_from_array(&values[0], idx)?;
            if !val.is_null() {
                self.moving_min.push(val);
            }
        }
        if let Some(res) = self.moving_min.min() {
            self.min = res.clone();
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for idx in 0..values[0].len() {
            let val = ScalarValue::try_from_array(&values[0], idx)?;
            if !val.is_null() {
                (self.moving_min).pop();
            }
        }
        if let Some(res) = self.moving_min.min() {
            self.min = res.clone();
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.min.clone())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.min) + self.min.size()
    }
}

//
// Moving min and moving max
// The implementation is taken from https://github.com/spebern/moving_min_max/blob/master/src/lib.rs.

// Keep track of the minimum or maximum value in a sliding window.
//
// `moving min max` provides one data structure for keeping track of the
// minimum value and one for keeping track of the maximum value in a sliding
// window.
//
// Each element is stored with the current min/max. One stack to push and another one for pop. If pop stack is empty,
// push to this stack all elements popped from first stack while updating their current min/max. Now pop from
// the second stack (MovingMin/Max struct works as a queue). To find the minimum element of the queue,
// look at the smallest/largest two elements of the individual stacks, then take the minimum of those two values.
//
// The complexity of the operations are
// - O(1) for getting the minimum/maximum
// - O(1) for push
// - amortized O(1) for pop

/// ```
/// # use datafusion_functions_aggregate::min_max::MovingMin;
/// let mut moving_min = MovingMin::<i32>::new();
/// moving_min.push(2);
/// moving_min.push(1);
/// moving_min.push(3);
///
/// assert_eq!(moving_min.min(), Some(&1));
/// assert_eq!(moving_min.pop(), Some(2));
///
/// assert_eq!(moving_min.min(), Some(&1));
/// assert_eq!(moving_min.pop(), Some(1));
///
/// assert_eq!(moving_min.min(), Some(&3));
/// assert_eq!(moving_min.pop(), Some(3));
///
/// assert_eq!(moving_min.min(), None);
/// assert_eq!(moving_min.pop(), None);
/// ```
#[derive(Debug)]
pub struct MovingMin<T> {
    push_stack: Vec<(T, T)>,
    pop_stack: Vec<(T, T)>,
}

impl<T: Clone + PartialOrd> Default for MovingMin<T> {
    fn default() -> Self {
        Self {
            push_stack: Vec::new(),
            pop_stack: Vec::new(),
        }
    }
}

impl<T: Clone + PartialOrd> MovingMin<T> {
    /// Creates a new `MovingMin` to keep track of the minimum in a sliding
    /// window.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new `MovingMin` to keep track of the minimum in a sliding
    /// window with `capacity` allocated slots.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            push_stack: Vec::with_capacity(capacity),
            pop_stack: Vec::with_capacity(capacity),
        }
    }

    /// Returns the minimum of the sliding window or `None` if the window is
    /// empty.
    #[inline]
    pub fn min(&self) -> Option<&T> {
        match (self.push_stack.last(), self.pop_stack.last()) {
            (None, None) => None,
            (Some((_, min)), None) => Some(min),
            (None, Some((_, min))) => Some(min),
            (Some((_, a)), Some((_, b))) => Some(if a < b { a } else { b }),
        }
    }

    /// Pushes a new element into the sliding window.
    #[inline]
    pub fn push(&mut self, val: T) {
        self.push_stack.push(match self.push_stack.last() {
            Some((_, min)) => {
                if val > *min {
                    (val, min.clone())
                } else {
                    (val.clone(), val)
                }
            }
            None => (val.clone(), val),
        });
    }

    /// Removes and returns the last value of the sliding window.
    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        if self.pop_stack.is_empty() {
            match self.push_stack.pop() {
                Some((val, _)) => {
                    let mut last = (val.clone(), val);
                    self.pop_stack.push(last.clone());
                    while let Some((val, _)) = self.push_stack.pop() {
                        let min = if last.1 < val {
                            last.1.clone()
                        } else {
                            val.clone()
                        };
                        last = (val.clone(), min);
                        self.pop_stack.push(last.clone());
                    }
                }
                None => return None,
            }
        }
        self.pop_stack.pop().map(|(val, _)| val)
    }

    /// Returns the number of elements stored in the sliding window.
    #[inline]
    pub fn len(&self) -> usize {
        self.push_stack.len() + self.pop_stack.len()
    }

    /// Returns `true` if the moving window contains no elements.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
/// ```
/// # use datafusion_functions_aggregate::min_max::MovingMax;
/// let mut moving_max = MovingMax::<i32>::new();
/// moving_max.push(2);
/// moving_max.push(3);
/// moving_max.push(1);
///
/// assert_eq!(moving_max.max(), Some(&3));
/// assert_eq!(moving_max.pop(), Some(2));
///
/// assert_eq!(moving_max.max(), Some(&3));
/// assert_eq!(moving_max.pop(), Some(3));
///
/// assert_eq!(moving_max.max(), Some(&1));
/// assert_eq!(moving_max.pop(), Some(1));
///
/// assert_eq!(moving_max.max(), None);
/// assert_eq!(moving_max.pop(), None);
/// ```
#[derive(Debug)]
pub struct MovingMax<T> {
    push_stack: Vec<(T, T)>,
    pop_stack: Vec<(T, T)>,
}

impl<T: Clone + PartialOrd> Default for MovingMax<T> {
    fn default() -> Self {
        Self {
            push_stack: Vec::new(),
            pop_stack: Vec::new(),
        }
    }
}

impl<T: Clone + PartialOrd> MovingMax<T> {
    /// Creates a new `MovingMax` to keep track of the maximum in a sliding window.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new `MovingMax` to keep track of the maximum in a sliding window with
    /// `capacity` allocated slots.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            push_stack: Vec::with_capacity(capacity),
            pop_stack: Vec::with_capacity(capacity),
        }
    }

    /// Returns the maximum of the sliding window or `None` if the window is empty.
    #[inline]
    pub fn max(&self) -> Option<&T> {
        match (self.push_stack.last(), self.pop_stack.last()) {
            (None, None) => None,
            (Some((_, max)), None) => Some(max),
            (None, Some((_, max))) => Some(max),
            (Some((_, a)), Some((_, b))) => Some(if a > b { a } else { b }),
        }
    }

    /// Pushes a new element into the sliding window.
    #[inline]
    pub fn push(&mut self, val: T) {
        self.push_stack.push(match self.push_stack.last() {
            Some((_, max)) => {
                if val < *max {
                    (val, max.clone())
                } else {
                    (val.clone(), val)
                }
            }
            None => (val.clone(), val),
        });
    }

    /// Removes and returns the last value of the sliding window.
    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        if self.pop_stack.is_empty() {
            match self.push_stack.pop() {
                Some((val, _)) => {
                    let mut last = (val.clone(), val);
                    self.pop_stack.push(last.clone());
                    while let Some((val, _)) = self.push_stack.pop() {
                        let max = if last.1 > val {
                            last.1.clone()
                        } else {
                            val.clone()
                        };
                        last = (val.clone(), max);
                        self.pop_stack.push(last.clone());
                    }
                }
                None => return None,
            }
        }
        self.pop_stack.pop().map(|(val, _)| val)
    }

    /// Returns the number of elements stored in the sliding window.
    #[inline]
    pub fn len(&self) -> usize {
        self.push_stack.len() + self.pop_stack.len()
    }

    /// Returns `true` if the moving window contains no elements.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

make_udaf_expr_and_func!(
    Max,
    max,
    expression,
    "Returns the maximum of a group of values.",
    max_udaf
);

make_udaf_expr_and_func!(
    Min,
    min,
    expression,
    "Returns the minimum of a group of values.",
    min_udaf
);

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{
        IntervalDayTimeType, IntervalMonthDayNanoType, IntervalYearMonthType,
    };
    use std::sync::Arc;

    #[test]
    fn interval_min_max() {
        // IntervalYearMonth
        let b = IntervalYearMonthArray::from(vec![
            IntervalYearMonthType::make_value(0, 1),
            IntervalYearMonthType::make_value(5, 34),
            IntervalYearMonthType::make_value(-2, 4),
            IntervalYearMonthType::make_value(7, -4),
            IntervalYearMonthType::make_value(0, 1),
        ]);
        let b: ArrayRef = Arc::new(b);

        let mut min =
            MinAccumulator::try_new(&DataType::Interval(IntervalUnit::YearMonth))
                .unwrap();
        min.update_batch(&[Arc::clone(&b)]).unwrap();
        let min_res = min.evaluate().unwrap();
        assert_eq!(
            min_res,
            ScalarValue::IntervalYearMonth(Some(IntervalYearMonthType::make_value(
                -2, 4
            )))
        );

        let mut max =
            MaxAccumulator::try_new(&DataType::Interval(IntervalUnit::YearMonth))
                .unwrap();
        max.update_batch(&[Arc::clone(&b)]).unwrap();
        let max_res = max.evaluate().unwrap();
        assert_eq!(
            max_res,
            ScalarValue::IntervalYearMonth(Some(IntervalYearMonthType::make_value(
                5, 34
            )))
        );

        // IntervalDayTime
        let b = IntervalDayTimeArray::from(vec![
            IntervalDayTimeType::make_value(0, 0),
            IntervalDayTimeType::make_value(5, 454000),
            IntervalDayTimeType::make_value(-34, 0),
            IntervalDayTimeType::make_value(7, -4000),
            IntervalDayTimeType::make_value(1, 0),
        ]);
        let b: ArrayRef = Arc::new(b);

        let mut min =
            MinAccumulator::try_new(&DataType::Interval(IntervalUnit::DayTime)).unwrap();
        min.update_batch(&[Arc::clone(&b)]).unwrap();
        let min_res = min.evaluate().unwrap();
        assert_eq!(
            min_res,
            ScalarValue::IntervalDayTime(Some(IntervalDayTimeType::make_value(-34, 0)))
        );

        let mut max =
            MaxAccumulator::try_new(&DataType::Interval(IntervalUnit::DayTime)).unwrap();
        max.update_batch(&[Arc::clone(&b)]).unwrap();
        let max_res = max.evaluate().unwrap();
        assert_eq!(
            max_res,
            ScalarValue::IntervalDayTime(Some(IntervalDayTimeType::make_value(7, -4000)))
        );

        // IntervalMonthDayNano
        let b = IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNanoType::make_value(1, 0, 0),
            IntervalMonthDayNanoType::make_value(344, 34, -43_000_000_000),
            IntervalMonthDayNanoType::make_value(-593, -33, 13_000_000_000),
            IntervalMonthDayNanoType::make_value(5, 2, 493_000_000_000),
            IntervalMonthDayNanoType::make_value(1, 0, 0),
        ]);
        let b: ArrayRef = Arc::new(b);

        let mut min =
            MinAccumulator::try_new(&DataType::Interval(IntervalUnit::MonthDayNano))
                .unwrap();
        min.update_batch(&[Arc::clone(&b)]).unwrap();
        let min_res = min.evaluate().unwrap();
        assert_eq!(
            min_res,
            ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNanoType::make_value(-593, -33, 13_000_000_000)
            ))
        );

        let mut max =
            MaxAccumulator::try_new(&DataType::Interval(IntervalUnit::MonthDayNano))
                .unwrap();
        max.update_batch(&[Arc::clone(&b)]).unwrap();
        let max_res = max.evaluate().unwrap();
        assert_eq!(
            max_res,
            ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNanoType::make_value(344, 34, -43_000_000_000)
            ))
        );
    }

    #[test]
    fn float_min_max_with_nans() {
        let pos_nan = f32::NAN;
        let zero = 0_f32;
        let neg_inf = f32::NEG_INFINITY;

        let check = |acc: &mut dyn Accumulator, values: &[&[f32]], expected: f32| {
            for batch in values.iter() {
                let batch =
                    Arc::new(Float32Array::from_iter_values(batch.iter().copied()));
                acc.update_batch(&[batch]).unwrap();
            }
            let result = acc.evaluate().unwrap();
            assert_eq!(result, ScalarValue::Float32(Some(expected)));
        };

        // This test checks both comparison between batches (which uses the min_max macro
        // defined above) and within a batch (which uses the arrow min/max compute function
        // and verifies both respect the total order comparison for floats)

        let min = || MinAccumulator::try_new(&DataType::Float32).unwrap();
        let max = || MaxAccumulator::try_new(&DataType::Float32).unwrap();

        check(&mut min(), &[&[zero], &[pos_nan]], zero);
        check(&mut min(), &[&[zero, pos_nan]], zero);
        check(&mut min(), &[&[zero], &[neg_inf]], neg_inf);
        check(&mut min(), &[&[zero, neg_inf]], neg_inf);
        check(&mut max(), &[&[zero], &[pos_nan]], pos_nan);
        check(&mut max(), &[&[zero, pos_nan]], pos_nan);
        check(&mut max(), &[&[zero], &[neg_inf]], zero);
        check(&mut max(), &[&[zero, neg_inf]], zero);
    }

    use datafusion_common::Result;
    use rand::Rng;

    fn get_random_vec_i32(len: usize) -> Vec<i32> {
        let mut rng = rand::thread_rng();
        let mut input = Vec::with_capacity(len);
        for _i in 0..len {
            input.push(rng.gen_range(0..100));
        }
        input
    }

    fn moving_min_i32(len: usize, n_sliding_window: usize) -> Result<()> {
        let data = get_random_vec_i32(len);
        let mut expected = Vec::with_capacity(len);
        let mut moving_min = MovingMin::<i32>::new();
        let mut res = Vec::with_capacity(len);
        for i in 0..len {
            let start = i.saturating_sub(n_sliding_window);
            expected.push(*data[start..i + 1].iter().min().unwrap());

            moving_min.push(data[i]);
            if i > n_sliding_window {
                moving_min.pop();
            }
            res.push(*moving_min.min().unwrap());
        }
        assert_eq!(res, expected);
        Ok(())
    }

    fn moving_max_i32(len: usize, n_sliding_window: usize) -> Result<()> {
        let data = get_random_vec_i32(len);
        let mut expected = Vec::with_capacity(len);
        let mut moving_max = MovingMax::<i32>::new();
        let mut res = Vec::with_capacity(len);
        for i in 0..len {
            let start = i.saturating_sub(n_sliding_window);
            expected.push(*data[start..i + 1].iter().max().unwrap());

            moving_max.push(data[i]);
            if i > n_sliding_window {
                moving_max.pop();
            }
            res.push(*moving_max.max().unwrap());
        }
        assert_eq!(res, expected);
        Ok(())
    }

    #[test]
    fn moving_min_tests() -> Result<()> {
        moving_min_i32(100, 10)?;
        moving_min_i32(100, 20)?;
        moving_min_i32(100, 50)?;
        moving_min_i32(100, 100)?;
        Ok(())
    }

    #[test]
    fn moving_max_tests() -> Result<()> {
        moving_max_i32(100, 10)?;
        moving_max_i32(100, 20)?;
        moving_max_i32(100, 50)?;
        moving_max_i32(100, 100)?;
        Ok(())
    }

    #[test]
    fn test_min_max_coerce_types() {
        // the coerced types is same with input types
        let funs: Vec<Box<dyn AggregateUDFImpl>> =
            vec![Box::new(Min::new()), Box::new(Max::new())];
        let input_types = vec![
            vec![DataType::Int32],
            vec![DataType::Decimal128(10, 2)],
            vec![DataType::Decimal256(1, 1)],
            vec![DataType::Utf8],
        ];
        for fun in funs {
            for input_type in &input_types {
                let result = fun.coerce_types(input_type);
                assert_eq!(*input_type, result.unwrap());
            }
        }
    }

    #[test]
    fn test_get_min_max_return_type_coerce_dictionary() -> Result<()> {
        let data_type =
            DataType::Dictionary(Box::new(DataType::Utf8), Box::new(DataType::Int32));
        let result = get_min_max_result_type(&[data_type])?;
        assert_eq!(result, vec![DataType::Int32]);
        Ok(())
    }
}
