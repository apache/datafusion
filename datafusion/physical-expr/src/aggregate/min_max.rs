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

//! Defines physical expressions that can evaluated at runtime during query execution

use std::any::Any;
use std::convert::TryFrom;
use std::sync::Arc;

use crate::{AggregateExpr, GroupsAccumulator, PhysicalExpr};
use arrow::compute;
use arrow::datatypes::{
    DataType, Date32Type, Date64Type, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use arrow::{
    array::{
        ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
        Int16Array, Int32Array, Int64Array, Int8Array, LargeStringArray, StringArray,
        Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
        Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array,
        UInt64Array, UInt8Array,
    },
    datatypes::Field,
};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{ArrowNumericType, PrimitiveArray};
use datafusion_common::ScalarValue;
use datafusion_common::{downcast_value, DataFusionError, Result};
use datafusion_expr::Accumulator;
use log::debug;

use crate::aggregate::row_accumulator::{
    is_row_accumulator_support_dtype, RowAccumulator,
};
use crate::aggregate::utils::down_cast_any_ref;
use crate::expressions::format_state_name;
use arrow::array::Array;
use arrow::array::Decimal128Array;
use datafusion_row::accessor::RowAccessor;

use super::groups_accumulator::accumulate::NullState;
use super::moving_min_max;
use super::utils::adjust_output_array;

// Min/max aggregation can take Dictionary encode input but always produces unpacked
// (aka non Dictionary) output. We need to adjust the output data type to reflect this.
// The reason min/max aggregate produces unpacked output because there is only one
// min/max value per group; there is no needs to keep them Dictionary encode
fn min_max_aggregate_data_type(input_type: DataType) -> DataType {
    if let DataType::Dictionary(_, value_type) = input_type {
        *value_type
    } else {
        input_type
    }
}

/// MAX aggregate expression
#[derive(Debug, Clone)]
pub struct Max {
    name: String,
    data_type: DataType,
    nullable: bool,
    expr: Arc<dyn PhysicalExpr>,
}

impl Max {
    /// Create a new MAX aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        Self {
            name: name.into(),
            expr,
            data_type: min_max_aggregate_data_type(data_type),
            nullable: true,
        }
    }
}

macro_rules! instantiate_min_max_accumulator {
    ($SELF:expr, $NUMERICTYPE:ident, $MIN:expr) => {{
        Ok(Box::new(MinMaxGroupsPrimitiveAccumulator::<
            $NUMERICTYPE,
            $MIN,
        >::new(&$SELF.data_type)))
    }};
}

impl AggregateExpr for Max {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(
            &self.name,
            self.data_type.clone(),
            self.nullable,
        ))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            format_state_name(&self.name, "max"),
            self.data_type.clone(),
            true,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(MaxAccumulator::try_new(&self.data_type)?))
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn row_accumulator_supported(&self) -> bool {
        is_row_accumulator_support_dtype(&self.data_type)
    }

    fn groups_accumulator_supported(&self) -> bool {
        use DataType::*;
        matches!(
            self.data_type,
            Int8 | Int16
                | Int32
                | Int64
                | UInt8
                | UInt16
                | UInt32
                | UInt64
                | Float32
                | Float64
                | Decimal128(_, _)
                | Date32
                | Date64
                | Time32(_)
                | Time64(_)
                | Timestamp(_, _)
        )
    }

    fn create_row_accumulator(
        &self,
        start_index: usize,
    ) -> Result<Box<dyn RowAccumulator>> {
        Ok(Box::new(MaxRowAccumulator::new(
            start_index,
            self.data_type.clone(),
        )))
    }

    fn create_groups_accumulator(&self) -> Result<Box<dyn GroupsAccumulator>> {
        use DataType::*;
        use TimeUnit::*;

        match self.data_type {
            Int8 => instantiate_min_max_accumulator!(self, Int8Type, false),
            Int16 => instantiate_min_max_accumulator!(self, Int16Type, false),
            Int32 => instantiate_min_max_accumulator!(self, Int32Type, false),
            Int64 => instantiate_min_max_accumulator!(self, Int64Type, false),
            UInt8 => instantiate_min_max_accumulator!(self, UInt8Type, false),
            UInt16 => instantiate_min_max_accumulator!(self, UInt16Type, false),
            UInt32 => instantiate_min_max_accumulator!(self, UInt32Type, false),
            UInt64 => instantiate_min_max_accumulator!(self, UInt64Type, false),
            Float32 => {
                instantiate_min_max_accumulator!(self, Float32Type, false)
            }
            Float64 => {
                instantiate_min_max_accumulator!(self, Float64Type, false)
            }
            Date32 => instantiate_min_max_accumulator!(self, Date32Type, false),
            Date64 => instantiate_min_max_accumulator!(self, Date64Type, false),
            Time32(Second) => {
                instantiate_min_max_accumulator!(self, Time32SecondType, false)
            }
            Time32(Millisecond) => {
                instantiate_min_max_accumulator!(self, Time32MillisecondType, false)
            }
            Time64(Microsecond) => {
                instantiate_min_max_accumulator!(self, Time64MicrosecondType, false)
            }
            Time64(Nanosecond) => {
                instantiate_min_max_accumulator!(self, Time64NanosecondType, false)
            }
            Timestamp(Second, _) => {
                instantiate_min_max_accumulator!(self, TimestampSecondType, false)
            }
            Timestamp(Millisecond, _) => {
                instantiate_min_max_accumulator!(self, TimestampMillisecondType, false)
            }
            Timestamp(Microsecond, _) => {
                instantiate_min_max_accumulator!(self, TimestampMicrosecondType, false)
            }
            Timestamp(Nanosecond, _) => {
                instantiate_min_max_accumulator!(self, TimestampNanosecondType, false)
            }

            // It would be nice to have a fast implementation for Strings as well
            // https://github.com/apache/arrow-datafusion/issues/6906
            Decimal128(_, _) => Ok(Box::new(MinMaxGroupsPrimitiveAccumulator::<
                Decimal128Type,
                false,
            >::new(&self.data_type))),
            // This is only reached if groups_accumulator_supported is out of sync
            _ => Err(DataFusionError::Internal(format!(
                "MinMaxGroupsPrimitiveAccumulator not supported for max({})",
                self.data_type
            ))),
        }
    }

    fn reverse_expr(&self) -> Option<Arc<dyn AggregateExpr>> {
        Some(Arc::new(self.clone()))
    }

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SlidingMaxAccumulator::try_new(&self.data_type)?))
    }
}

impl PartialEq<dyn Any> for Max {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.data_type == x.data_type
                    && self.nullable == x.nullable
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

// Statically-typed version of min/max(array) -> ScalarValue for string types.
macro_rules! typed_min_max_batch_string {
    ($VALUES:expr, $ARRAYTYPE:ident, $SCALAR:ident, $OP:ident) => {{
        let array = downcast_value!($VALUES, $ARRAYTYPE);
        let value = compute::$OP(array);
        let value = value.and_then(|e| Some(e.to_string()));
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
            // all types that have a natural order
            DataType::Float64 => {
                typed_min_max_batch!($VALUES, Float64Array, Float64, $OP)
            }
            DataType::Float32 => {
                typed_min_max_batch!($VALUES, Float32Array, Float32, $OP)
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
            other => {
                // This should have been handled before
                return Err(DataFusionError::Internal(format!(
                    "Min/Max accumulator not implemented for type {:?}",
                    other
                )));
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
        DataType::Boolean => {
            typed_min_max_batch!(values, BooleanArray, Boolean, min_boolean)
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
        DataType::Boolean => {
            typed_min_max_batch!(values, BooleanArray, Boolean, max_boolean)
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

// min/max of two non-string scalar values.
macro_rules! typed_min_max_v2 {
    ($INDEX:ident, $ACC:ident, $SCALAR:expr, $TYPE:ident, $OP:ident) => {{
        paste::item! {
            match $SCALAR {
                None => {}
                Some(v) => $ACC.[<$OP _ $TYPE>]($INDEX, *v as $TYPE)
            }
        }
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

macro_rules! interval_choose_min_max {
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
            Some(interval_choose_min_max!($OP)) => $RHS.clone(),
            Some(_) => $LHS.clone(),
            None => {
                return Err(DataFusionError::Internal(
                    "Comparison error while computing interval min/max".to_string(),
                ))
            }
        }
    }};
}

// min/max of two scalar values of the same type
macro_rules! min_max {
    ($VALUE:expr, $DELTA:expr, $OP:ident) => {{
        Ok(match ($VALUE, $DELTA) {
            (
                lhs @ ScalarValue::Decimal128(lhsv, lhsp, lhss),
                rhs @ ScalarValue::Decimal128(rhsv, rhsp, rhss)
            ) => {
                if lhsp.eq(rhsp) && lhss.eq(rhss) {
                    typed_min_max!(lhsv, rhsv, Decimal128, $OP, lhsp, lhss)
                } else {
                    return Err(DataFusionError::Internal(format!(
                    "MIN/MAX is not expected to receive scalars of incompatible types {:?}",
                    (lhs, rhs)
                )));
                }
            }
            (ScalarValue::Boolean(lhs), ScalarValue::Boolean(rhs)) => {
                typed_min_max!(lhs, rhs, Boolean, $OP)
            }
            (ScalarValue::Float64(lhs), ScalarValue::Float64(rhs)) => {
                typed_min_max!(lhs, rhs, Float64, $OP)
            }
            (ScalarValue::Float32(lhs), ScalarValue::Float32(rhs)) => {
                typed_min_max!(lhs, rhs, Float32, $OP)
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
            e => {
                return Err(DataFusionError::Internal(format!(
                    "MIN/MAX is not expected to receive scalars of incompatible types {:?}",
                    e
                )))
            }
        })
    }};
}

// min/max of two scalar values of the same type
macro_rules! min_max_v2 {
    ($INDEX:ident, $ACC:ident, $SCALAR:expr, $OP:ident) => {{
        Ok(match $SCALAR {
            ScalarValue::Boolean(rhs) => {
                typed_min_max_v2!($INDEX, $ACC, rhs, bool, $OP)
            }
            ScalarValue::Float64(rhs) => {
                typed_min_max_v2!($INDEX, $ACC, rhs, f64, $OP)
            }
            ScalarValue::Float32(rhs) => {
                typed_min_max_v2!($INDEX, $ACC, rhs, f32, $OP)
            }
            ScalarValue::UInt64(rhs) => {
                typed_min_max_v2!($INDEX, $ACC, rhs, u64, $OP)
            }
            ScalarValue::UInt32(rhs) => {
                typed_min_max_v2!($INDEX, $ACC, rhs, u32, $OP)
            }
            ScalarValue::UInt16(rhs) => {
                typed_min_max_v2!($INDEX, $ACC, rhs, u16, $OP)
            }
            ScalarValue::UInt8(rhs) => {
                typed_min_max_v2!($INDEX, $ACC, rhs, u8, $OP)
            }
            ScalarValue::Int64(rhs) => {
                typed_min_max_v2!($INDEX, $ACC, rhs, i64, $OP)
            }
            ScalarValue::Int32(rhs) => {
                typed_min_max_v2!($INDEX, $ACC, rhs, i32, $OP)
            }
            ScalarValue::Int16(rhs) => {
                typed_min_max_v2!($INDEX, $ACC, rhs, i16, $OP)
            }
            ScalarValue::Int8(rhs) => {
                typed_min_max_v2!($INDEX, $ACC, rhs, i8, $OP)
            }
            ScalarValue::Decimal128(rhs, ..) => {
                typed_min_max_v2!($INDEX, $ACC, rhs, i128, $OP)
            }
            ScalarValue::Null => {
                // do nothing
            }
            e => {
                return Err(DataFusionError::Internal(format!(
                    "MIN/MAX is not expected to receive scalars of incompatible types {:?}",
                    e
                )))
            }
        })
    }};
}

/// the minimum of two scalar values
pub fn min(lhs: &ScalarValue, rhs: &ScalarValue) -> Result<ScalarValue> {
    min_max!(lhs, rhs, min)
}

pub fn min_row(index: usize, accessor: &mut RowAccessor, s: &ScalarValue) -> Result<()> {
    min_max_v2!(index, accessor, s, min)
}

/// the maximum of two scalar values
pub fn max(lhs: &ScalarValue, rhs: &ScalarValue) -> Result<ScalarValue> {
    min_max!(lhs, rhs, max)
}

pub fn max_row(index: usize, accessor: &mut RowAccessor, s: &ScalarValue) -> Result<()> {
    min_max_v2!(index, accessor, s, max)
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
        self.max = max(&self.max, delta)?;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.max.clone()])
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(self.max.clone())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.max) + self.max.size()
    }
}

/// An accumulator to compute the maximum value
#[derive(Debug)]
pub struct SlidingMaxAccumulator {
    max: ScalarValue,
    moving_max: moving_min_max::MovingMax<ScalarValue>,
}

impl SlidingMaxAccumulator {
    /// new max accumulator
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        Ok(Self {
            max: ScalarValue::try_from(datatype)?,
            moving_max: moving_min_max::MovingMax::<ScalarValue>::new(),
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

    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.max.clone()])
    }

    fn evaluate(&self) -> Result<ScalarValue> {
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
struct MaxRowAccumulator {
    index: usize,
    data_type: DataType,
}

impl MaxRowAccumulator {
    pub fn new(index: usize, data_type: DataType) -> Self {
        Self { index, data_type }
    }
}

impl RowAccumulator for MaxRowAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        let values = &values[0];
        let delta = &max_batch(values)?;
        max_row(self.index, accessor, delta)
    }

    fn update_scalar_values(
        &mut self,
        values: &[ScalarValue],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        let value = &values[0];
        max_row(self.index, accessor, value)
    }

    fn update_scalar(
        &mut self,
        value: &ScalarValue,
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        max_row(self.index, accessor, value)
    }

    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        self.update_batch(states, accessor)
    }

    fn evaluate(&self, accessor: &RowAccessor) -> Result<ScalarValue> {
        Ok(accessor.get_as_scalar(&self.data_type, self.index))
    }

    #[inline(always)]
    fn state_index(&self) -> usize {
        self.index
    }
}

/// MIN aggregate expression
#[derive(Debug, Clone)]
pub struct Min {
    name: String,
    data_type: DataType,
    nullable: bool,
    expr: Arc<dyn PhysicalExpr>,
}

impl Min {
    /// Create a new MIN aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        Self {
            name: name.into(),
            expr,
            data_type: min_max_aggregate_data_type(data_type),
            nullable: true,
        }
    }
}

impl AggregateExpr for Min {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(
            &self.name,
            self.data_type.clone(),
            self.nullable,
        ))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(MinAccumulator::try_new(&self.data_type)?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            format_state_name(&self.name, "min"),
            self.data_type.clone(),
            true,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn row_accumulator_supported(&self) -> bool {
        is_row_accumulator_support_dtype(&self.data_type)
    }

    fn create_row_accumulator(
        &self,
        start_index: usize,
    ) -> Result<Box<dyn RowAccumulator>> {
        Ok(Box::new(MinRowAccumulator::new(
            start_index,
            self.data_type.clone(),
        )))
    }

    fn groups_accumulator_supported(&self) -> bool {
        use DataType::*;
        matches!(
            self.data_type,
            Int8 | Int16
                | Int32
                | Int64
                | UInt8
                | UInt16
                | UInt32
                | UInt64
                | Float32
                | Float64
                | Decimal128(_, _)
                | Date32
                | Date64
                | Time32(_)
                | Time64(_)
                | Timestamp(_, _)
        )
    }

    fn create_groups_accumulator(&self) -> Result<Box<dyn GroupsAccumulator>> {
        use DataType::*;
        use TimeUnit::*;
        match self.data_type {
            Int8 => instantiate_min_max_accumulator!(self, Int8Type, true),
            Int16 => instantiate_min_max_accumulator!(self, Int16Type, true),
            Int32 => instantiate_min_max_accumulator!(self, Int32Type, true),
            Int64 => instantiate_min_max_accumulator!(self, Int64Type, true),
            UInt8 => instantiate_min_max_accumulator!(self, UInt8Type, true),
            UInt16 => instantiate_min_max_accumulator!(self, UInt16Type, true),
            UInt32 => instantiate_min_max_accumulator!(self, UInt32Type, true),
            UInt64 => instantiate_min_max_accumulator!(self, UInt64Type, true),
            Float32 => {
                instantiate_min_max_accumulator!(self, Float32Type, true)
            }
            Float64 => {
                instantiate_min_max_accumulator!(self, Float64Type, true)
            }
            Date32 => instantiate_min_max_accumulator!(self, Date32Type, true),
            Date64 => instantiate_min_max_accumulator!(self, Date64Type, true),
            Time32(Second) => {
                instantiate_min_max_accumulator!(self, Time32SecondType, true)
            }
            Time32(Millisecond) => {
                instantiate_min_max_accumulator!(self, Time32MillisecondType, true)
            }
            Time64(Microsecond) => {
                instantiate_min_max_accumulator!(self, Time64MicrosecondType, true)
            }
            Time64(Nanosecond) => {
                instantiate_min_max_accumulator!(self, Time64NanosecondType, true)
            }
            Timestamp(Second, _) => {
                instantiate_min_max_accumulator!(self, TimestampSecondType, true)
            }
            Timestamp(Millisecond, _) => {
                instantiate_min_max_accumulator!(self, TimestampMillisecondType, true)
            }
            Timestamp(Microsecond, _) => {
                instantiate_min_max_accumulator!(self, TimestampMicrosecondType, true)
            }
            Timestamp(Nanosecond, _) => {
                instantiate_min_max_accumulator!(self, TimestampNanosecondType, true)
            }
            Decimal128(_, _) => Ok(Box::new(MinMaxGroupsPrimitiveAccumulator::<
                Decimal128Type,
                true,
            >::new(&self.data_type))),
            // This is only reached if groups_accumulator_supported is out of sync
            _ => Err(DataFusionError::Internal(format!(
                "MinMaxGroupsPrimitiveAccumulator not supported for min({})",
                self.data_type
            ))),
        }
    }

    fn reverse_expr(&self) -> Option<Arc<dyn AggregateExpr>> {
        Some(Arc::new(self.clone()))
    }

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SlidingMinAccumulator::try_new(&self.data_type)?))
    }
}

impl PartialEq<dyn Any> for Min {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.data_type == x.data_type
                    && self.nullable == x.nullable
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
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
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.min.clone()])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];
        let delta = &min_batch(values)?;
        self.min = min(&self.min, delta)?;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(self.min.clone())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.min) + self.min.size()
    }
}

/// An accumulator to compute the minimum value
#[derive(Debug)]
pub struct SlidingMinAccumulator {
    min: ScalarValue,
    moving_min: moving_min_max::MovingMin<ScalarValue>,
}

impl SlidingMinAccumulator {
    /// new min accumulator
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        Ok(Self {
            min: ScalarValue::try_from(datatype)?,
            moving_min: moving_min_max::MovingMin::<ScalarValue>::new(),
        })
    }
}

impl Accumulator for SlidingMinAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
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

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(self.min.clone())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.min) + self.min.size()
    }
}

#[derive(Debug)]
struct MinRowAccumulator {
    index: usize,
    data_type: DataType,
}

impl MinRowAccumulator {
    pub fn new(index: usize, data_type: DataType) -> Self {
        Self { index, data_type }
    }
}

impl RowAccumulator for MinRowAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        let values = &values[0];
        let delta = &min_batch(values)?;
        min_row(self.index, accessor, delta)?;
        Ok(())
    }

    fn update_scalar_values(
        &mut self,
        values: &[ScalarValue],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        let value = &values[0];
        min_row(self.index, accessor, value)
    }

    fn update_scalar(
        &mut self,
        value: &ScalarValue,
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        min_row(self.index, accessor, value)
    }

    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        self.update_batch(states, accessor)
    }

    fn evaluate(&self, accessor: &RowAccessor) -> Result<ScalarValue> {
        Ok(accessor.get_as_scalar(&self.data_type, self.index))
    }

    #[inline(always)]
    fn state_index(&self) -> usize {
        self.index
    }
}

trait MinMax {
    fn min() -> Self;
    fn max() -> Self;
}

impl MinMax for u8 {
    fn min() -> Self {
        u8::MIN
    }
    fn max() -> Self {
        u8::MAX
    }
}
impl MinMax for i8 {
    fn min() -> Self {
        i8::MIN
    }
    fn max() -> Self {
        i8::MAX
    }
}
impl MinMax for u16 {
    fn min() -> Self {
        u16::MIN
    }
    fn max() -> Self {
        u16::MAX
    }
}
impl MinMax for i16 {
    fn min() -> Self {
        i16::MIN
    }
    fn max() -> Self {
        i16::MAX
    }
}
impl MinMax for u32 {
    fn min() -> Self {
        u32::MIN
    }
    fn max() -> Self {
        u32::MAX
    }
}
impl MinMax for i32 {
    fn min() -> Self {
        i32::MIN
    }
    fn max() -> Self {
        i32::MAX
    }
}
impl MinMax for i64 {
    fn min() -> Self {
        i64::MIN
    }
    fn max() -> Self {
        i64::MAX
    }
}
impl MinMax for u64 {
    fn min() -> Self {
        u64::MIN
    }
    fn max() -> Self {
        u64::MAX
    }
}
impl MinMax for f32 {
    fn min() -> Self {
        f32::MIN
    }
    fn max() -> Self {
        f32::MAX
    }
}
impl MinMax for f64 {
    fn min() -> Self {
        f64::MIN
    }
    fn max() -> Self {
        f64::MAX
    }
}
impl MinMax for i128 {
    fn min() -> Self {
        i128::MIN
    }
    fn max() -> Self {
        i128::MAX
    }
}

/// An accumulator to compute the min or max of a [`PrimitiveArray<T>`].
///
/// Stores values as native/primitive type
///
/// Note this doesn't use [`PrimitiveGroupsAccumulator`] because it
/// needs to control the default accumulator value (which is not
/// `default::Default()`)
///
/// [`PrimitiveGroupsAccumulator`]: crate::aggregate::groups_accumulator::prim_op::PrimitiveGroupsAccumulator
#[derive(Debug)]
struct MinMaxGroupsPrimitiveAccumulator<T, const MIN: bool>
where
    T: ArrowNumericType + Send,
    T::Native: MinMax,
{
    /// Min/max per group, stored as the native type
    min_max: Vec<T::Native>,

    /// Track nulls in the input / filters
    null_state: NullState,

    /// The output datatype (needed for decimal precision/scale)
    data_type: DataType,
}

impl<T, const MIN: bool> MinMaxGroupsPrimitiveAccumulator<T, MIN>
where
    T: ArrowNumericType + Send,
    T::Native: MinMax,
{
    pub fn new(data_type: &DataType) -> Self {
        debug!(
            "MinMaxGroupsPrimitiveAccumulator ({}, {})",
            std::any::type_name::<T>(),
            MIN,
        );

        Self {
            min_max: vec![],
            null_state: NullState::new(),
            data_type: data_type.clone(),
        }
    }
}

impl<T, const MIN: bool> GroupsAccumulator for MinMaxGroupsPrimitiveAccumulator<T, MIN>
where
    T: ArrowNumericType + Send,
    T::Native: MinMax,
{
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&arrow_array::BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");
        let values = values[0].as_primitive::<T>();

        self.min_max.resize(
            total_num_groups,
            if MIN {
                T::Native::max()
            } else {
                T::Native::min()
            },
        );

        // NullState dispatches / handles tracking nulls and groups that saw no values
        self.null_state.accumulate(
            group_indices,
            values,
            opt_filter,
            total_num_groups,
            |group_index, new_value| {
                let val = &mut self.min_max[group_index];
                match MIN {
                    true => {
                        if new_value < *val {
                            *val = new_value;
                        }
                    }
                    false => {
                        if new_value > *val {
                            *val = new_value;
                        }
                    }
                }
            },
        );

        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&arrow_array::BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        Self::update_batch(self, values, group_indices, opt_filter, total_num_groups)
    }

    fn evaluate(&mut self) -> Result<ArrayRef> {
        let min_max = std::mem::take(&mut self.min_max);
        let nulls = self.null_state.build();

        let min_max = PrimitiveArray::<T>::new(min_max.into(), Some(nulls)); // no copy
        let min_max = adjust_output_array(&self.data_type, Arc::new(min_max))?;

        Ok(Arc::new(min_max))
    }

    // return arrays for min/max values
    fn state(&mut self) -> Result<Vec<ArrayRef>> {
        let nulls = self.null_state.build();

        let min_max = std::mem::take(&mut self.min_max);
        let min_max = PrimitiveArray::<T>::new(min_max.into(), Some(nulls)); // zero copy

        let min_max = adjust_output_array(&self.data_type, Arc::new(min_max))?;

        Ok(vec![min_max])
    }

    fn size(&self) -> usize {
        self.min_max.capacity() * std::mem::size_of::<T>() + self.null_state.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::col;
    use crate::expressions::tests::aggregate;
    use crate::generic_test_op;
    use arrow::datatypes::*;
    use arrow::record_batch::RecordBatch;
    use datafusion_common::Result;
    use datafusion_common::ScalarValue;
    use datafusion_common::ScalarValue::Decimal128;

    #[test]
    fn min_decimal() -> Result<()> {
        // min
        let left = ScalarValue::Decimal128(Some(123), 10, 2);
        let right = ScalarValue::Decimal128(Some(124), 10, 2);
        let result = min(&left, &right)?;
        assert_eq!(result, left);

        // min batch
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(Some)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );

        let result = min_batch(&array)?;
        assert_eq!(result, ScalarValue::Decimal128(Some(1), 10, 0));

        // min batch without values
        let array: ArrayRef = Arc::new(
            std::iter::repeat::<Option<i128>>(None)
                .take(0)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );
        let result = min_batch(&array)?;
        assert_eq!(ScalarValue::Decimal128(None, 10, 0), result);

        // min batch with agg
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(Some)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );
        generic_test_op!(
            array,
            DataType::Decimal128(10, 0),
            Min,
            ScalarValue::Decimal128(Some(1), 10, 0)
        )
    }

    #[test]
    fn min_decimal_all_nulls() -> Result<()> {
        // min batch all nulls
        let array: ArrayRef = Arc::new(
            std::iter::repeat::<Option<i128>>(None)
                .take(6)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );
        generic_test_op!(
            array,
            DataType::Decimal128(10, 0),
            Min,
            ScalarValue::Decimal128(None, 10, 0)
        )
    }

    #[test]
    fn min_decimal_with_nulls() -> Result<()> {
        // min batch with nulls
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(|i| if i == 2 { None } else { Some(i) })
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );

        generic_test_op!(
            array,
            DataType::Decimal128(10, 0),
            Min,
            ScalarValue::Decimal128(Some(1), 10, 0)
        )
    }

    #[test]
    fn max_decimal() -> Result<()> {
        // max
        let left = ScalarValue::Decimal128(Some(123), 10, 2);
        let right = ScalarValue::Decimal128(Some(124), 10, 2);
        let result = max(&left, &right)?;
        assert_eq!(result, right);

        let right = ScalarValue::Decimal128(Some(124), 10, 3);
        let result = max(&left, &right);
        let expect = DataFusionError::Internal(format!(
            "MIN/MAX is not expected to receive scalars of incompatible types {:?}",
            (Decimal128(Some(123), 10, 2), Decimal128(Some(124), 10, 3))
        ));
        assert_eq!(expect.to_string(), result.unwrap_err().to_string());

        // max batch
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(Some)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 5)?,
        );
        let result = max_batch(&array)?;
        assert_eq!(result, ScalarValue::Decimal128(Some(5), 10, 5));

        // max batch without values
        let array: ArrayRef = Arc::new(
            std::iter::repeat::<Option<i128>>(None)
                .take(0)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );
        let result = max_batch(&array)?;
        assert_eq!(ScalarValue::Decimal128(None, 10, 0), result);

        // max batch with agg
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(Some)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );
        generic_test_op!(
            array,
            DataType::Decimal128(10, 0),
            Max,
            ScalarValue::Decimal128(Some(5), 10, 0)
        )
    }

    #[test]
    fn max_decimal_with_nulls() -> Result<()> {
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(|i| if i == 2 { None } else { Some(i) })
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );
        generic_test_op!(
            array,
            DataType::Decimal128(10, 0),
            Max,
            ScalarValue::Decimal128(Some(5), 10, 0)
        )
    }

    #[test]
    fn max_decimal_all_nulls() -> Result<()> {
        let array: ArrayRef = Arc::new(
            std::iter::repeat::<Option<i128>>(None)
                .take(6)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );
        generic_test_op!(
            array,
            DataType::Decimal128(10, 0),
            Min,
            ScalarValue::Decimal128(None, 10, 0)
        )
    }

    #[test]
    fn max_i32() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(a, DataType::Int32, Max, ScalarValue::from(5i32))
    }

    #[test]
    fn min_i32() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(a, DataType::Int32, Min, ScalarValue::from(1i32))
    }

    #[test]
    fn max_utf8() -> Result<()> {
        let a: ArrayRef = Arc::new(StringArray::from(vec!["d", "a", "c", "b"]));
        generic_test_op!(
            a,
            DataType::Utf8,
            Max,
            ScalarValue::Utf8(Some("d".to_string()))
        )
    }

    #[test]
    fn max_large_utf8() -> Result<()> {
        let a: ArrayRef = Arc::new(LargeStringArray::from(vec!["d", "a", "c", "b"]));
        generic_test_op!(
            a,
            DataType::LargeUtf8,
            Max,
            ScalarValue::LargeUtf8(Some("d".to_string()))
        )
    }

    #[test]
    fn min_utf8() -> Result<()> {
        let a: ArrayRef = Arc::new(StringArray::from(vec!["d", "a", "c", "b"]));
        generic_test_op!(
            a,
            DataType::Utf8,
            Min,
            ScalarValue::Utf8(Some("a".to_string()))
        )
    }

    #[test]
    fn min_large_utf8() -> Result<()> {
        let a: ArrayRef = Arc::new(LargeStringArray::from(vec!["d", "a", "c", "b"]));
        generic_test_op!(
            a,
            DataType::LargeUtf8,
            Min,
            ScalarValue::LargeUtf8(Some("a".to_string()))
        )
    }

    #[test]
    fn max_i32_with_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            Some(4),
            Some(5),
        ]));
        generic_test_op!(a, DataType::Int32, Max, ScalarValue::from(5i32))
    }

    #[test]
    fn min_i32_with_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            Some(4),
            Some(5),
        ]));
        generic_test_op!(a, DataType::Int32, Min, ScalarValue::from(1i32))
    }

    #[test]
    fn max_i32_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        generic_test_op!(a, DataType::Int32, Max, ScalarValue::Int32(None))
    }

    #[test]
    fn min_i32_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        generic_test_op!(a, DataType::Int32, Min, ScalarValue::Int32(None))
    }

    #[test]
    fn max_u32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(UInt32Array::from(vec![1_u32, 2_u32, 3_u32, 4_u32, 5_u32]));
        generic_test_op!(a, DataType::UInt32, Max, ScalarValue::from(5_u32))
    }

    #[test]
    fn min_u32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(UInt32Array::from(vec![1_u32, 2_u32, 3_u32, 4_u32, 5_u32]));
        generic_test_op!(a, DataType::UInt32, Min, ScalarValue::from(1u32))
    }

    #[test]
    fn max_f32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float32Array::from(vec![1_f32, 2_f32, 3_f32, 4_f32, 5_f32]));
        generic_test_op!(a, DataType::Float32, Max, ScalarValue::from(5_f32))
    }

    #[test]
    fn min_f32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float32Array::from(vec![1_f32, 2_f32, 3_f32, 4_f32, 5_f32]));
        generic_test_op!(a, DataType::Float32, Min, ScalarValue::from(1_f32))
    }

    #[test]
    fn max_f64() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64, 4_f64, 5_f64]));
        generic_test_op!(a, DataType::Float64, Max, ScalarValue::from(5_f64))
    }

    #[test]
    fn min_f64() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64, 4_f64, 5_f64]));
        generic_test_op!(a, DataType::Float64, Min, ScalarValue::from(1_f64))
    }

    #[test]
    fn min_date32() -> Result<()> {
        let a: ArrayRef = Arc::new(Date32Array::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(a, DataType::Date32, Min, ScalarValue::Date32(Some(1)))
    }

    #[test]
    fn min_date64() -> Result<()> {
        let a: ArrayRef = Arc::new(Date64Array::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(a, DataType::Date64, Min, ScalarValue::Date64(Some(1)))
    }

    #[test]
    fn max_date32() -> Result<()> {
        let a: ArrayRef = Arc::new(Date32Array::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(a, DataType::Date32, Max, ScalarValue::Date32(Some(5)))
    }

    #[test]
    fn max_date64() -> Result<()> {
        let a: ArrayRef = Arc::new(Date64Array::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(a, DataType::Date64, Max, ScalarValue::Date64(Some(5)))
    }

    #[test]
    fn min_time32second() -> Result<()> {
        let a: ArrayRef = Arc::new(Time32SecondArray::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(
            a,
            DataType::Time32(TimeUnit::Second),
            Min,
            ScalarValue::Time32Second(Some(1))
        )
    }

    #[test]
    fn max_time32second() -> Result<()> {
        let a: ArrayRef = Arc::new(Time32SecondArray::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(
            a,
            DataType::Time32(TimeUnit::Second),
            Max,
            ScalarValue::Time32Second(Some(5))
        )
    }

    #[test]
    fn min_time32millisecond() -> Result<()> {
        let a: ArrayRef = Arc::new(Time32MillisecondArray::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(
            a,
            DataType::Time32(TimeUnit::Millisecond),
            Min,
            ScalarValue::Time32Millisecond(Some(1))
        )
    }

    #[test]
    fn max_time32millisecond() -> Result<()> {
        let a: ArrayRef = Arc::new(Time32MillisecondArray::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(
            a,
            DataType::Time32(TimeUnit::Millisecond),
            Max,
            ScalarValue::Time32Millisecond(Some(5))
        )
    }

    #[test]
    fn min_time64microsecond() -> Result<()> {
        let a: ArrayRef = Arc::new(Time64MicrosecondArray::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(
            a,
            DataType::Time64(TimeUnit::Microsecond),
            Min,
            ScalarValue::Time64Microsecond(Some(1))
        )
    }

    #[test]
    fn max_time64microsecond() -> Result<()> {
        let a: ArrayRef = Arc::new(Time64MicrosecondArray::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(
            a,
            DataType::Time64(TimeUnit::Microsecond),
            Max,
            ScalarValue::Time64Microsecond(Some(5))
        )
    }

    #[test]
    fn min_time64nanosecond() -> Result<()> {
        let a: ArrayRef = Arc::new(Time64NanosecondArray::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(
            a,
            DataType::Time64(TimeUnit::Nanosecond),
            Min,
            ScalarValue::Time64Nanosecond(Some(1))
        )
    }

    #[test]
    fn max_time64nanosecond() -> Result<()> {
        let a: ArrayRef = Arc::new(Time64NanosecondArray::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(
            a,
            DataType::Time64(TimeUnit::Nanosecond),
            Max,
            ScalarValue::Time64Nanosecond(Some(5))
        )
    }

    #[test]
    fn max_bool() -> Result<()> {
        let a: ArrayRef = Arc::new(BooleanArray::from(vec![false, false]));
        generic_test_op!(a, DataType::Boolean, Max, ScalarValue::from(false))?;

        let a: ArrayRef = Arc::new(BooleanArray::from(vec![true, true]));
        generic_test_op!(a, DataType::Boolean, Max, ScalarValue::from(true))?;

        let a: ArrayRef = Arc::new(BooleanArray::from(vec![false, true, false]));
        generic_test_op!(a, DataType::Boolean, Max, ScalarValue::from(true))?;

        let a: ArrayRef = Arc::new(BooleanArray::from(vec![true, false, true]));
        generic_test_op!(a, DataType::Boolean, Max, ScalarValue::from(true))?;

        let a: ArrayRef = Arc::new(BooleanArray::from(Vec::<bool>::new()));
        generic_test_op!(
            a,
            DataType::Boolean,
            Max,
            ScalarValue::from(None as Option<bool>)
        )?;

        let a: ArrayRef = Arc::new(BooleanArray::from(vec![None as Option<bool>]));
        generic_test_op!(
            a,
            DataType::Boolean,
            Max,
            ScalarValue::from(None as Option<bool>)
        )?;

        let a: ArrayRef =
            Arc::new(BooleanArray::from(vec![None, Some(true), Some(false)]));
        generic_test_op!(a, DataType::Boolean, Max, ScalarValue::from(true))?;

        Ok(())
    }

    #[test]
    fn min_bool() -> Result<()> {
        let a: ArrayRef = Arc::new(BooleanArray::from(vec![false, false]));
        generic_test_op!(a, DataType::Boolean, Min, ScalarValue::from(false))?;

        let a: ArrayRef = Arc::new(BooleanArray::from(vec![true, true]));
        generic_test_op!(a, DataType::Boolean, Min, ScalarValue::from(true))?;

        let a: ArrayRef = Arc::new(BooleanArray::from(vec![false, true, false]));
        generic_test_op!(a, DataType::Boolean, Min, ScalarValue::from(false))?;

        let a: ArrayRef = Arc::new(BooleanArray::from(vec![true, false, true]));
        generic_test_op!(a, DataType::Boolean, Min, ScalarValue::from(false))?;

        let a: ArrayRef = Arc::new(BooleanArray::from(Vec::<bool>::new()));
        generic_test_op!(
            a,
            DataType::Boolean,
            Min,
            ScalarValue::from(None as Option<bool>)
        )?;

        let a: ArrayRef = Arc::new(BooleanArray::from(vec![None as Option<bool>]));
        generic_test_op!(
            a,
            DataType::Boolean,
            Min,
            ScalarValue::from(None as Option<bool>)
        )?;

        let a: ArrayRef =
            Arc::new(BooleanArray::from(vec![None, Some(true), Some(false)]));
        generic_test_op!(a, DataType::Boolean, Min, ScalarValue::from(false))?;

        Ok(())
    }
}
