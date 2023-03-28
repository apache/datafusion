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

use crate::physical_expr::down_cast_any_ref;
use crate::PhysicalExpr;
use arrow::array::{Array, ArrayData, ArrayRef, ArrowPrimitiveType, PrimitiveArray};
use arrow::compute::{binary, unary};
use arrow::datatypes::{
    ArrowNativeTypeOp, DataType, Date32Type, Date64Type, IntervalDayTimeType,
    IntervalMonthDayNanoType, IntervalYearMonthType, Schema, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use arrow::record_batch::RecordBatch;
use arrow::util::bit_mask::combine_option_bitmap;
use arrow_buffer::Buffer;
use arrow_schema::{ArrowError, IntervalUnit};
use chrono::NaiveDateTime;
use datafusion_common::cast::*;
use datafusion_common::scalar::*;
use datafusion_common::Result;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::type_coercion::binary::coerce_types;
use datafusion_expr::{ColumnarValue, Operator};
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// Perform DATE/TIME/TIMESTAMP +/ INTERVAL math
#[derive(Debug)]
pub struct DateTimeIntervalExpr {
    lhs: Arc<dyn PhysicalExpr>,
    op: Operator,
    rhs: Arc<dyn PhysicalExpr>,
    // TODO: move type checking to the planning phase and not in the physical expr
    // so we can remove this
    input_schema: Schema,
}

impl DateTimeIntervalExpr {
    /// Create a new instance of DateIntervalExpr
    pub fn try_new(
        lhs: Arc<dyn PhysicalExpr>,
        op: Operator,
        rhs: Arc<dyn PhysicalExpr>,
        input_schema: &Schema,
    ) -> Result<Self> {
        match (
            lhs.data_type(input_schema)?,
            op,
            rhs.data_type(input_schema)?,
        ) {
            (
                DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _),
                Operator::Plus | Operator::Minus,
                DataType::Interval(_),
            )
            | (DataType::Timestamp(_, _), Operator::Minus, DataType::Timestamp(_, _))
            | (
                DataType::Interval(_),
                Operator::Plus | Operator::Minus,
                DataType::Interval(_),
            ) => Ok(Self {
                lhs,
                op,
                rhs,
                input_schema: input_schema.clone(),
            }),
            (lhs, _, rhs) => Err(DataFusionError::Execution(format!(
                "Invalid operation between '{lhs}' and '{rhs}' for DateIntervalExpr"
            ))),
        }
    }

    /// Get the left-hand side expression
    pub fn lhs(&self) -> &Arc<dyn PhysicalExpr> {
        &self.lhs
    }

    /// Get the operator
    pub fn op(&self) -> &Operator {
        &self.op
    }

    /// Get the right-hand side expression
    pub fn rhs(&self) -> &Arc<dyn PhysicalExpr> {
        &self.rhs
    }
}

impl Display for DateTimeIntervalExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.lhs, self.op, self.rhs)
    }
}

impl PhysicalExpr for DateTimeIntervalExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        coerce_types(
            &self.lhs.data_type(input_schema)?,
            &Operator::Minus,
            &self.rhs.data_type(input_schema)?,
        )
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.lhs.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let lhs_value = self.lhs.evaluate(batch)?;
        let rhs_value = self.rhs.evaluate(batch)?;
        // Invert sign for subtraction
        let sign = match self.op {
            Operator::Plus => 1,
            Operator::Minus => -1,
            _ => {
                // this should be unreachable because we check the operators in `try_new`
                let msg = "Invalid operator for DateIntervalExpr";
                return Err(DataFusionError::Internal(msg.to_string()));
            }
        };
        // RHS is first checked. If it is a Scalar, there are 2 options:
        // Either LHS is also a Scalar and matching operation is applied,
        // or LHS is an Array and unary operations for related types are
        // applied in evaluate_array function. If RHS is an Array, then
        // LHS must also be, moreover; they must be the same Timestamp type.
        match (lhs_value, rhs_value) {
            (ColumnarValue::Scalar(operand_lhs), ColumnarValue::Scalar(operand_rhs)) => {
                Ok(ColumnarValue::Scalar(if sign > 0 {
                    operand_lhs.add(&operand_rhs)?
                } else {
                    operand_lhs.sub(&operand_rhs)?
                }))
            }
            (ColumnarValue::Array(array_lhs), ColumnarValue::Scalar(operand_rhs)) => {
                evaluate_array(array_lhs, sign, &operand_rhs)
            }

            (ColumnarValue::Array(array_lhs), ColumnarValue::Array(array_rhs)) => {
                evaluate_temporal_arrays(&array_lhs, sign, &array_rhs)
            }
            (_, _) => {
                let msg = "If RHS of the operation is an array, then LHS also must be";
                Err(DataFusionError::Internal(msg.to_string()))
            }
        }
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.lhs.clone(), self.rhs.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(DateTimeIntervalExpr::try_new(
            children[0].clone(),
            self.op,
            children[1].clone(),
            &self.input_schema,
        )?))
    }
}

impl PartialEq<dyn Any> for DateTimeIntervalExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.lhs.eq(&x.lhs) && self.op == x.op && self.rhs.eq(&x.rhs))
            .unwrap_or(false)
    }
}

pub fn evaluate_array(
    array: ArrayRef,
    sign: i32,
    scalar: &ScalarValue,
) -> Result<ColumnarValue> {
    let ret = match array.data_type() {
        DataType::Date32 => {
            let array = as_date32_array(&array)?;
            Arc::new(unary::<Date32Type, _, Date32Type>(array, |days| {
                date32_add(days, scalar, sign).unwrap()
            })) as ArrayRef
        }
        DataType::Date64 => {
            let array = as_date64_array(&array)?;
            Arc::new(unary::<Date64Type, _, Date64Type>(array, |ms| {
                date64_add(ms, scalar, sign).unwrap()
            })) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let array = as_timestamp_second_array(&array)?;
            Arc::new(unary::<TimestampSecondType, _, TimestampSecondType>(
                array,
                |ts_s| seconds_add(ts_s, scalar, sign).unwrap(),
            )) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let array = as_timestamp_millisecond_array(&array)?;
            Arc::new(
                unary::<TimestampMillisecondType, _, TimestampMillisecondType>(
                    array,
                    |ts_ms| milliseconds_add(ts_ms, scalar, sign).unwrap(),
                ),
            ) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let array = as_timestamp_microsecond_array(&array)?;
            Arc::new(
                unary::<TimestampMicrosecondType, _, TimestampMicrosecondType>(
                    array,
                    |ts_us| microseconds_add(ts_us, scalar, sign).unwrap(),
                ),
            ) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let array = as_timestamp_nanosecond_array(&array)?;
            Arc::new(
                unary::<TimestampNanosecondType, _, TimestampNanosecondType>(
                    array,
                    |ts_ns| nanoseconds_add(ts_ns, scalar, sign).unwrap(),
                ),
            ) as ArrayRef
        }
        _ => Err(DataFusionError::Execution(format!(
            "Invalid lhs type for DateIntervalExpr: {}",
            array.data_type()
        )))?,
    };
    Ok(ColumnarValue::Array(ret))
}

macro_rules! ts_sub_op {
    ($lhs:ident, $rhs:ident, $lhs_tz:ident, $rhs_tz:ident, $coef:expr, $caster:expr, $op:expr, $ts_unit:expr, $mode:expr, $type_in:ty, $type_out:ty) => {{
        let prim_array_lhs = $caster(&$lhs)?;
        let prim_array_rhs = $caster(&$rhs)?;
        let ret = Arc::new(try_binary_op::<$type_in, $type_in, _, $type_out>(
            prim_array_lhs,
            prim_array_rhs,
            |ts1, ts2| {
                Ok($op(
                    $ts_unit(&with_timezone_to_naive_datetime::<$mode>(
                        ts1.mul_wrapping($coef),
                        &$lhs_tz,
                    )?),
                    $ts_unit(&with_timezone_to_naive_datetime::<$mode>(
                        ts2.mul_wrapping($coef),
                        &$rhs_tz,
                    )?),
                ))
            },
        )?) as ArrayRef;
        ret
    }};
}
macro_rules! interval_op {
    ($lhs:ident, $rhs:ident, $caster:expr, $op:expr, $sign:ident, $type_in:ty) => {{
        let prim_array_lhs = $caster(&$lhs)?;
        let prim_array_rhs = $caster(&$rhs)?;
        let ret = Arc::new(binary::<$type_in, $type_in, _, $type_in>(
            prim_array_lhs,
            prim_array_rhs,
            |interval1, interval2| $op(interval1, interval2, $sign),
        )?) as ArrayRef;
        ret
    }};
}
macro_rules! interval_cross_op {
    ($lhs:ident, $rhs:ident, $caster1:expr, $caster2:expr, $op:expr, $sign:ident, $commute:ident, $type_in1:ty, $type_in2:ty) => {{
        let prim_array_lhs = $caster1(&$lhs)?;
        let prim_array_rhs = $caster2(&$rhs)?;
        let ret = Arc::new(binary::<$type_in1, $type_in2, _, IntervalMonthDayNanoType>(
            prim_array_lhs,
            prim_array_rhs,
            |interval1, interval2| $op(interval1, interval2, $sign, $commute),
        )?) as ArrayRef;
        ret
    }};
}
macro_rules! ts_interval_op {
    ($lhs:ident, $rhs:ident, $caster1:expr, $caster2:expr, $op:expr, $sign:ident, $type_in1:ty, $type_in2:ty) => {{
        let prim_array_lhs = $caster1(&$lhs)?;
        let prim_array_rhs = $caster2(&$rhs)?;
        let ret = Arc::new(binary::<$type_in1, $type_in2, _, $type_in1>(
            prim_array_lhs,
            prim_array_rhs,
            |ts, interval| {
                $op(ts, interval as i128, $sign)
                    .expect("error in {$sign} operation of interval with timestamp")
            },
        )?) as ArrayRef;
        ret
    }};
}
// This function evaluates temporal array operations, such as timestamp - timestamp, interval + interval,
// timestamp + interval, and interval + timestamp. It takes two arrays as input and an integer sign representing
// the operation (+1 for addition and -1 for subtraction). It returns a ColumnarValue as output, which can hold
// either a scalar or an array.
pub fn evaluate_temporal_arrays(
    array_lhs: &ArrayRef,
    sign: i32,
    array_rhs: &ArrayRef,
) -> Result<ColumnarValue> {
    let ret = match (array_lhs.data_type(), array_rhs.data_type()) {
        // Timestamp - Timestamp operations, operands of only the same types are supported.
        (DataType::Timestamp(_, _), DataType::Timestamp(_, _)) => {
            ts_array_op(array_lhs, array_rhs)?
        }
        // Interval (+ , -) Interval operations
        (DataType::Interval(_), DataType::Interval(_)) => {
            interval_array_op(array_lhs, array_rhs, sign)?
        }
        // Timestamp (+ , -) Interval and Interval + Timestamp operations
        // Interval - Timestamp operation is not rational hence not supported
        (DataType::Timestamp(_, _), DataType::Interval(_)) => {
            ts_interval_array_op(array_lhs, sign, array_rhs)?
        }
        (DataType::Interval(_), DataType::Timestamp(_, _)) if sign == 1 => {
            ts_interval_array_op(array_rhs, sign, array_lhs)?
        }
        (_, _) => Err(DataFusionError::Execution(format!(
            "Invalid array types for DateIntervalExpr: {:?} {} {:?}",
            array_lhs.data_type(),
            sign,
            array_rhs.data_type()
        )))?,
    };
    Ok(ColumnarValue::Array(ret))
}

#[inline]
unsafe fn build_primitive_array<O: ArrowPrimitiveType>(
    len: usize,
    buffer: Buffer,
    null_count: usize,
    null_buffer: Option<Buffer>,
) -> PrimitiveArray<O> {
    PrimitiveArray::from(ArrayData::new_unchecked(
        O::DATA_TYPE,
        len,
        Some(null_count),
        null_buffer,
        0,
        vec![buffer],
        vec![],
    ))
}

pub fn try_binary_op<A, B, F, O>(
    a: &PrimitiveArray<A>,
    b: &PrimitiveArray<B>,
    op: F,
) -> Result<PrimitiveArray<O>, ArrowError>
where
    A: ArrowPrimitiveType,
    B: ArrowPrimitiveType,
    O: ArrowPrimitiveType,
    F: Fn(A::Native, B::Native) -> Result<O::Native, ArrowError>,
{
    if a.len() != b.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform binary operation on arrays of different length".to_string(),
        ));
    }
    let len = a.len();

    if a.is_empty() {
        return Ok(PrimitiveArray::from(ArrayData::new_empty(&O::DATA_TYPE)));
    }

    let null_buffer = combine_option_bitmap(&[a.data(), b.data()], len);
    let null_count = null_buffer
        .as_ref()
        .map(|x| len - x.count_set_bits_offset(0, len))
        .unwrap_or_default();

    let values = a.values().iter().zip(b.values()).map(|(l, r)| op(*l, *r));
    // JUSTIFICATION
    //  Benefit
    //      ~60% speedup
    //  Soundness
    //      `values` is an iterator with a known size from a PrimitiveArray
    let buffer = unsafe { Buffer::try_from_trusted_len_iter(values) }?;

    Ok(unsafe { build_primitive_array(len, buffer, null_count, null_buffer) })
}

/// Performs a timestamp subtraction operation on two arrays and returns the resulting array.
fn ts_array_op(array_lhs: &ArrayRef, array_rhs: &ArrayRef) -> Result<ArrayRef> {
    match (array_lhs.data_type(), array_rhs.data_type()) {
        (
            DataType::Timestamp(TimeUnit::Second, opt_tz_lhs),
            DataType::Timestamp(TimeUnit::Second, opt_tz_rhs),
        ) => Ok(ts_sub_op!(
            array_lhs,
            array_rhs,
            opt_tz_lhs,
            opt_tz_rhs,
            1000i64,
            as_timestamp_second_array,
            seconds_sub,
            NaiveDateTime::timestamp,
            MILLISECOND_MODE,
            TimestampSecondType,
            IntervalDayTimeType
        )),
        (
            DataType::Timestamp(TimeUnit::Millisecond, opt_tz_lhs),
            DataType::Timestamp(TimeUnit::Millisecond, opt_tz_rhs),
        ) => Ok(ts_sub_op!(
            array_lhs,
            array_rhs,
            opt_tz_lhs,
            opt_tz_rhs,
            1i64,
            as_timestamp_millisecond_array,
            milliseconds_sub,
            NaiveDateTime::timestamp_millis,
            MILLISECOND_MODE,
            TimestampMillisecondType,
            IntervalDayTimeType
        )),
        (
            DataType::Timestamp(TimeUnit::Microsecond, opt_tz_lhs),
            DataType::Timestamp(TimeUnit::Microsecond, opt_tz_rhs),
        ) => Ok(ts_sub_op!(
            array_lhs,
            array_rhs,
            opt_tz_lhs,
            opt_tz_rhs,
            1000i64,
            as_timestamp_microsecond_array,
            microseconds_sub,
            NaiveDateTime::timestamp_micros,
            NANOSECOND_MODE,
            TimestampMicrosecondType,
            IntervalMonthDayNanoType
        )),
        (
            DataType::Timestamp(TimeUnit::Nanosecond, opt_tz_lhs),
            DataType::Timestamp(TimeUnit::Nanosecond, opt_tz_rhs),
        ) => Ok(ts_sub_op!(
            array_lhs,
            array_rhs,
            opt_tz_lhs,
            opt_tz_rhs,
            1i64,
            as_timestamp_nanosecond_array,
            nanoseconds_sub,
            NaiveDateTime::timestamp_nanos,
            NANOSECOND_MODE,
            TimestampNanosecondType,
            IntervalMonthDayNanoType
        )),
        (_, _) => Err(DataFusionError::Execution(format!(
            "Invalid array types for Timestamp subtraction: {:?} - {:?}",
            array_lhs.data_type(),
            array_rhs.data_type()
        ))),
    }
}
/// Performs an interval operation on two arrays and returns the resulting array.
/// The operation sign determines whether to perform addition or subtraction.
/// The data type and unit of the two input arrays must match the supported combinations.
fn interval_array_op(
    array_lhs: &ArrayRef,
    array_rhs: &ArrayRef,
    sign: i32,
) -> Result<ArrayRef> {
    match (array_lhs.data_type(), array_rhs.data_type()) {
        (
            DataType::Interval(IntervalUnit::YearMonth),
            DataType::Interval(IntervalUnit::YearMonth),
        ) => Ok(interval_op!(
            array_lhs,
            array_rhs,
            as_interval_ym_array,
            op_ym,
            sign,
            IntervalYearMonthType
        )),
        (
            DataType::Interval(IntervalUnit::YearMonth),
            DataType::Interval(IntervalUnit::DayTime),
        ) => Ok(interval_cross_op!(
            array_lhs,
            array_rhs,
            as_interval_ym_array,
            as_interval_dt_array,
            op_ym_dt,
            sign,
            false,
            IntervalYearMonthType,
            IntervalDayTimeType
        )),
        (
            DataType::Interval(IntervalUnit::YearMonth),
            DataType::Interval(IntervalUnit::MonthDayNano),
        ) => Ok(interval_cross_op!(
            array_lhs,
            array_rhs,
            as_interval_ym_array,
            as_interval_mdn_array,
            op_ym_mdn,
            sign,
            false,
            IntervalYearMonthType,
            IntervalMonthDayNanoType
        )),
        (
            DataType::Interval(IntervalUnit::DayTime),
            DataType::Interval(IntervalUnit::YearMonth),
        ) => Ok(interval_cross_op!(
            array_rhs,
            array_lhs,
            as_interval_ym_array,
            as_interval_dt_array,
            op_ym_dt,
            sign,
            true,
            IntervalYearMonthType,
            IntervalDayTimeType
        )),
        (
            DataType::Interval(IntervalUnit::DayTime),
            DataType::Interval(IntervalUnit::DayTime),
        ) => Ok(interval_op!(
            array_lhs,
            array_rhs,
            as_interval_dt_array,
            op_dt,
            sign,
            IntervalDayTimeType
        )),
        (
            DataType::Interval(IntervalUnit::DayTime),
            DataType::Interval(IntervalUnit::MonthDayNano),
        ) => Ok(interval_cross_op!(
            array_lhs,
            array_rhs,
            as_interval_dt_array,
            as_interval_mdn_array,
            op_dt_mdn,
            sign,
            false,
            IntervalDayTimeType,
            IntervalMonthDayNanoType
        )),
        (
            DataType::Interval(IntervalUnit::MonthDayNano),
            DataType::Interval(IntervalUnit::YearMonth),
        ) => Ok(interval_cross_op!(
            array_rhs,
            array_lhs,
            as_interval_ym_array,
            as_interval_mdn_array,
            op_ym_mdn,
            sign,
            true,
            IntervalYearMonthType,
            IntervalMonthDayNanoType
        )),
        (
            DataType::Interval(IntervalUnit::MonthDayNano),
            DataType::Interval(IntervalUnit::DayTime),
        ) => Ok(interval_cross_op!(
            array_rhs,
            array_lhs,
            as_interval_dt_array,
            as_interval_mdn_array,
            op_dt_mdn,
            sign,
            true,
            IntervalDayTimeType,
            IntervalMonthDayNanoType
        )),
        (
            DataType::Interval(IntervalUnit::MonthDayNano),
            DataType::Interval(IntervalUnit::MonthDayNano),
        ) => Ok(interval_op!(
            array_lhs,
            array_rhs,
            as_interval_mdn_array,
            op_mdn,
            sign,
            IntervalMonthDayNanoType
        )),
        (_, _) => Err(DataFusionError::Execution(format!(
            "Invalid array types for Interval operation: {:?} {} {:?}",
            array_lhs.data_type(),
            sign,
            array_rhs.data_type()
        ))),
    }
}
/// Performs a timestamp/interval operation on two arrays and returns the resulting array.
/// The operation sign determines whether to perform addition or subtraction.
/// The data type and unit of the two input arrays must match the supported combinations.
fn ts_interval_array_op(
    array_lhs: &ArrayRef,
    sign: i32,
    array_rhs: &ArrayRef,
) -> Result<ArrayRef> {
    match (array_lhs.data_type(), array_rhs.data_type()) {
        (
            DataType::Timestamp(TimeUnit::Second, _),
            DataType::Interval(IntervalUnit::YearMonth),
        ) => Ok(ts_interval_op!(
            array_lhs,
            array_rhs,
            as_timestamp_second_array,
            as_interval_ym_array,
            seconds_add_array::<YM_MODE>,
            sign,
            TimestampSecondType,
            IntervalYearMonthType
        )),
        (
            DataType::Timestamp(TimeUnit::Second, _),
            DataType::Interval(IntervalUnit::DayTime),
        ) => Ok(ts_interval_op!(
            array_lhs,
            array_rhs,
            as_timestamp_second_array,
            as_interval_dt_array,
            seconds_add_array::<DT_MODE>,
            sign,
            TimestampSecondType,
            IntervalDayTimeType
        )),
        (
            DataType::Timestamp(TimeUnit::Second, _),
            DataType::Interval(IntervalUnit::MonthDayNano),
        ) => Ok(ts_interval_op!(
            array_lhs,
            array_rhs,
            as_timestamp_second_array,
            as_interval_mdn_array,
            seconds_add_array::<MDN_MODE>,
            sign,
            TimestampSecondType,
            IntervalMonthDayNanoType
        )),
        (
            DataType::Timestamp(TimeUnit::Millisecond, _),
            DataType::Interval(IntervalUnit::YearMonth),
        ) => Ok(ts_interval_op!(
            array_lhs,
            array_rhs,
            as_timestamp_millisecond_array,
            as_interval_ym_array,
            milliseconds_add_array::<YM_MODE>,
            sign,
            TimestampMillisecondType,
            IntervalYearMonthType
        )),
        (
            DataType::Timestamp(TimeUnit::Millisecond, _),
            DataType::Interval(IntervalUnit::DayTime),
        ) => Ok(ts_interval_op!(
            array_lhs,
            array_rhs,
            as_timestamp_millisecond_array,
            as_interval_dt_array,
            milliseconds_add_array::<DT_MODE>,
            sign,
            TimestampMillisecondType,
            IntervalDayTimeType
        )),
        (
            DataType::Timestamp(TimeUnit::Millisecond, _),
            DataType::Interval(IntervalUnit::MonthDayNano),
        ) => Ok(ts_interval_op!(
            array_lhs,
            array_rhs,
            as_timestamp_millisecond_array,
            as_interval_mdn_array,
            milliseconds_add_array::<MDN_MODE>,
            sign,
            TimestampMillisecondType,
            IntervalMonthDayNanoType
        )),
        (
            DataType::Timestamp(TimeUnit::Microsecond, _),
            DataType::Interval(IntervalUnit::YearMonth),
        ) => Ok(ts_interval_op!(
            array_lhs,
            array_rhs,
            as_timestamp_microsecond_array,
            as_interval_ym_array,
            microseconds_add_array::<YM_MODE>,
            sign,
            TimestampMicrosecondType,
            IntervalYearMonthType
        )),
        (
            DataType::Timestamp(TimeUnit::Microsecond, _),
            DataType::Interval(IntervalUnit::DayTime),
        ) => Ok(ts_interval_op!(
            array_lhs,
            array_rhs,
            as_timestamp_microsecond_array,
            as_interval_dt_array,
            microseconds_add_array::<DT_MODE>,
            sign,
            TimestampMicrosecondType,
            IntervalDayTimeType
        )),
        (
            DataType::Timestamp(TimeUnit::Microsecond, _),
            DataType::Interval(IntervalUnit::MonthDayNano),
        ) => Ok(ts_interval_op!(
            array_lhs,
            array_rhs,
            as_timestamp_microsecond_array,
            as_interval_mdn_array,
            microseconds_add_array::<MDN_MODE>,
            sign,
            TimestampMicrosecondType,
            IntervalMonthDayNanoType
        )),
        (
            DataType::Timestamp(TimeUnit::Nanosecond, _),
            DataType::Interval(IntervalUnit::YearMonth),
        ) => Ok(ts_interval_op!(
            array_lhs,
            array_rhs,
            as_timestamp_nanosecond_array,
            as_interval_ym_array,
            nanoseconds_add_array::<YM_MODE>,
            sign,
            TimestampNanosecondType,
            IntervalYearMonthType
        )),
        (
            DataType::Timestamp(TimeUnit::Nanosecond, _),
            DataType::Interval(IntervalUnit::DayTime),
        ) => Ok(ts_interval_op!(
            array_lhs,
            array_rhs,
            as_timestamp_nanosecond_array,
            as_interval_dt_array,
            nanoseconds_add_array::<DT_MODE>,
            sign,
            TimestampNanosecondType,
            IntervalDayTimeType
        )),
        (
            DataType::Timestamp(TimeUnit::Nanosecond, _),
            DataType::Interval(IntervalUnit::MonthDayNano),
        ) => Ok(ts_interval_op!(
            array_lhs,
            array_rhs,
            as_timestamp_nanosecond_array,
            as_interval_mdn_array,
            nanoseconds_add_array::<MDN_MODE>,
            sign,
            TimestampNanosecondType,
            IntervalMonthDayNanoType
        )),
        (_, _) => Err(DataFusionError::Execution(format!(
            "Invalid array types for Timestamp Interval operation: {:?} {} {:?}",
            array_lhs.data_type(),
            sign,
            array_rhs.data_type()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::create_physical_expr;
    use crate::execution_props::ExecutionProps;
    use arrow::array::{ArrayRef, Date32Builder};
    use arrow::datatypes::*;
    use chrono::{Duration, NaiveDate};
    use datafusion_common::delta::shift_months;
    use datafusion_common::{Column, Result, ToDFSchema};
    use datafusion_expr::Expr;
    use std::ops::Add;

    #[test]
    fn add_11_months() {
        let prior = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let actual = shift_months(prior, 11);
        assert_eq!(format!("{actual:?}").as_str(), "2000-12-01");
    }

    #[test]
    fn add_12_months() {
        let prior = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let actual = shift_months(prior, 12);
        assert_eq!(format!("{actual:?}").as_str(), "2001-01-01");
    }

    #[test]
    fn add_13_months() {
        let prior = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let actual = shift_months(prior, 13);
        assert_eq!(format!("{actual:?}").as_str(), "2001-02-01");
    }

    #[test]
    fn sub_11_months() {
        let prior = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let actual = shift_months(prior, -11);
        assert_eq!(format!("{actual:?}").as_str(), "1999-02-01");
    }

    #[test]
    fn sub_12_months() {
        let prior = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let actual = shift_months(prior, -12);
        assert_eq!(format!("{actual:?}").as_str(), "1999-01-01");
    }

    #[test]
    fn sub_13_months() {
        let prior = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let actual = shift_months(prior, -13);
        assert_eq!(format!("{actual:?}").as_str(), "1998-12-01");
    }

    #[test]
    fn add_32_day_time() -> Result<()> {
        // setup
        let dt = Expr::Literal(ScalarValue::Date32(Some(0)));
        let op = Operator::Plus;
        let interval = Expr::Literal(ScalarValue::new_interval_dt(1, 0));

        // exercise
        let res = exercise(&dt, op, &interval)?;

        // assert
        match res {
            ColumnarValue::Scalar(ScalarValue::Date32(Some(d))) => {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let res = epoch.add(Duration::days(d as i64));
                assert_eq!(format!("{res:?}").as_str(), "1970-01-02");
            }
            _ => Err(DataFusionError::NotImplemented(
                "Unexpected result!".to_string(),
            ))?,
        }

        Ok(())
    }

    #[test]
    fn sub_32_year_month() -> Result<()> {
        // setup
        let dt = Expr::Literal(ScalarValue::Date32(Some(0)));
        let op = Operator::Minus;
        let interval = Expr::Literal(ScalarValue::IntervalYearMonth(Some(13)));

        // exercise
        let res = exercise(&dt, op, &interval)?;

        // assert
        match res {
            ColumnarValue::Scalar(ScalarValue::Date32(Some(d))) => {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let res = epoch.add(Duration::days(d as i64));
                assert_eq!(format!("{res:?}").as_str(), "1968-12-01");
            }
            _ => Err(DataFusionError::NotImplemented(
                "Unexpected result!".to_string(),
            ))?,
        }

        Ok(())
    }

    #[test]
    fn add_64_day_time() -> Result<()> {
        // setup
        let dt = Expr::Literal(ScalarValue::Date64(Some(0)));
        let op = Operator::Plus;
        let interval =
            Expr::Literal(ScalarValue::new_interval_dt(-15, -24 * 60 * 60 * 1000));

        // exercise
        let res = exercise(&dt, op, &interval)?;

        // assert
        match res {
            ColumnarValue::Scalar(ScalarValue::Date64(Some(d))) => {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let res = epoch.add(Duration::milliseconds(d));
                assert_eq!(format!("{res:?}").as_str(), "1969-12-16");
            }
            _ => Err(DataFusionError::NotImplemented(
                "Unexpected result!".to_string(),
            ))?,
        }

        Ok(())
    }

    #[test]
    fn add_32_year_month() -> Result<()> {
        // setup
        let dt = Expr::Literal(ScalarValue::Date32(Some(0)));
        let op = Operator::Plus;
        let interval = Expr::Literal(ScalarValue::IntervalYearMonth(Some(1)));

        // exercise
        let res = exercise(&dt, op, &interval)?;

        // assert
        match res {
            ColumnarValue::Scalar(ScalarValue::Date32(Some(d))) => {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let res = epoch.add(Duration::days(d as i64));
                assert_eq!(format!("{res:?}").as_str(), "1970-02-01");
            }
            _ => Err(DataFusionError::NotImplemented(
                "Unexpected result!".to_string(),
            ))?,
        }

        Ok(())
    }

    #[test]
    fn add_32_month_day_nano() -> Result<()> {
        // setup
        let dt = Expr::Literal(ScalarValue::Date32(Some(0)));
        let op = Operator::Plus;
        let interval = Expr::Literal(ScalarValue::new_interval_mdn(-12, -15, -42));

        // exercise
        let res = exercise(&dt, op, &interval)?;

        // assert
        match res {
            ColumnarValue::Scalar(ScalarValue::Date32(Some(d))) => {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let res = epoch.add(Duration::days(d as i64));
                assert_eq!(format!("{res:?}").as_str(), "1968-12-17");
            }
            _ => Err(DataFusionError::NotImplemented(
                "Unexpected result!".to_string(),
            ))?,
        }

        Ok(())
    }

    #[test]
    fn add_1_millisecond() -> Result<()> {
        // setup
        let now_ts_ns = chrono::Utc::now().timestamp_nanos();
        let dt = Expr::Literal(ScalarValue::TimestampNanosecond(Some(now_ts_ns), None));
        let op = Operator::Plus;
        let interval = Expr::Literal(ScalarValue::new_interval_dt(0, 1));

        // exercise
        let res = exercise(&dt, op, &interval)?;

        // assert
        match res {
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(ts), None)) => {
                assert_eq!(ts, now_ts_ns + 1_000_000);
            }
            _ => Err(DataFusionError::NotImplemented(
                "Unexpected result!".to_string(),
            ))?,
        }
        Ok(())
    }

    #[test]
    fn add_2_hours() -> Result<()> {
        // setup
        let now_ts_s = chrono::Utc::now().timestamp();
        let dt = Expr::Literal(ScalarValue::TimestampSecond(Some(now_ts_s), None));
        let op = Operator::Plus;
        let interval = Expr::Literal(ScalarValue::new_interval_dt(0, 2 * 3600 * 1_000));

        // exercise
        let res = exercise(&dt, op, &interval)?;

        // assert
        match res {
            ColumnarValue::Scalar(ScalarValue::TimestampSecond(Some(ts), None)) => {
                assert_eq!(ts, now_ts_s + 2 * 3600);
            }
            _ => Err(DataFusionError::NotImplemented(
                "Unexpected result!".to_string(),
            ))?,
        }
        Ok(())
    }

    #[test]
    fn sub_4_hours() -> Result<()> {
        // setup
        let now_ts_s = chrono::Utc::now().timestamp();
        let dt = Expr::Literal(ScalarValue::TimestampSecond(Some(now_ts_s), None));
        let op = Operator::Minus;
        let interval = Expr::Literal(ScalarValue::new_interval_dt(0, 4 * 3600 * 1_000));

        // exercise
        let res = exercise(&dt, op, &interval)?;

        // assert
        match res {
            ColumnarValue::Scalar(ScalarValue::TimestampSecond(Some(ts), None)) => {
                assert_eq!(ts, now_ts_s - 4 * 3600);
            }
            _ => Err(DataFusionError::NotImplemented(
                "Unexpected result!".to_string(),
            ))?,
        }
        Ok(())
    }

    #[test]
    fn add_8_days() -> Result<()> {
        // setup
        let now_ts_ns = chrono::Utc::now().timestamp_nanos();
        let dt = Expr::Literal(ScalarValue::TimestampNanosecond(Some(now_ts_ns), None));
        let op = Operator::Plus;
        let interval = Expr::Literal(ScalarValue::new_interval_dt(8, 0));

        // exercise
        let res = exercise(&dt, op, &interval)?;

        // assert
        match res {
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(ts), None)) => {
                assert_eq!(ts, now_ts_ns + 8 * 86400 * 1_000_000_000);
            }
            _ => Err(DataFusionError::NotImplemented(
                "Unexpected result!".to_string(),
            ))?,
        }
        Ok(())
    }

    #[test]
    fn sub_16_days() -> Result<()> {
        // setup
        let now_ts_ns = chrono::Utc::now().timestamp_nanos();
        let dt = Expr::Literal(ScalarValue::TimestampNanosecond(Some(now_ts_ns), None));
        let op = Operator::Minus;
        let interval = Expr::Literal(ScalarValue::new_interval_dt(16, 0));

        // exercise
        let res = exercise(&dt, op, &interval)?;

        // assert
        match res {
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(ts), None)) => {
                assert_eq!(ts, now_ts_ns - 16 * 86400 * 1_000_000_000);
            }
            _ => Err(DataFusionError::NotImplemented(
                "Unexpected result!".to_string(),
            ))?,
        }
        Ok(())
    }

    #[test]
    fn array_add_26_days() -> Result<()> {
        let mut builder = Date32Builder::with_capacity(8);
        builder.append_slice(&[0, 1, 2, 3, 4, 5, 6, 7]);
        let a: ArrayRef = Arc::new(builder.finish());

        let schema = Schema::new(vec![Field::new("a", DataType::Date32, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![a])?;
        let dfs = schema.clone().to_dfschema()?;
        let props = ExecutionProps::new();

        let dt = Expr::Column(Column::from_name("a"));
        let interval = Expr::Literal(ScalarValue::new_interval_dt(26, 0));
        let op = Operator::Plus;

        let lhs = create_physical_expr(&dt, &dfs, &schema, &props)?;
        let rhs = create_physical_expr(&interval, &dfs, &schema, &props)?;

        let cut = DateTimeIntervalExpr::try_new(lhs, op, rhs, &schema)?;
        let res = cut.evaluate(&batch)?;

        let mut builder = Date32Builder::with_capacity(8);
        builder.append_slice(&[26, 27, 28, 29, 30, 31, 32, 33]);
        let expected: ArrayRef = Arc::new(builder.finish());

        // assert
        match res {
            ColumnarValue::Array(array) => {
                assert_eq!(&array, &expected)
            }
            _ => Err(DataFusionError::NotImplemented(
                "Unexpected result!".to_string(),
            ))?,
        }

        Ok(())
    }

    #[test]
    fn invalid_interval() -> Result<()> {
        // setup
        let dt = Expr::Literal(ScalarValue::Date32(Some(0)));
        let op = Operator::Plus;
        let interval = Expr::Literal(ScalarValue::Null);

        // exercise
        let res = exercise(&dt, op, &interval);
        assert!(res.is_err(), "Can't add a NULL interval");

        Ok(())
    }

    #[test]
    fn invalid_date() -> Result<()> {
        // setup
        let dt = Expr::Literal(ScalarValue::Null);
        let op = Operator::Plus;
        let interval = Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(0)));

        // exercise
        let res = exercise(&dt, op, &interval);
        assert!(res.is_err(), "Can't add to NULL date");

        Ok(())
    }

    #[test]
    fn invalid_op() -> Result<()> {
        // setup
        let dt = Expr::Literal(ScalarValue::Date32(Some(0)));
        let op = Operator::Eq;
        let interval = Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(0)));

        // exercise
        let res = exercise(&dt, op, &interval);
        assert!(res.is_err(), "Can't add dates with == operator");

        Ok(())
    }

    fn exercise(dt: &Expr, op: Operator, interval: &Expr) -> Result<ColumnarValue> {
        let mut builder = Date32Builder::with_capacity(1);
        builder.append_value(0);
        let a: ArrayRef = Arc::new(builder.finish());
        let schema = Schema::new(vec![Field::new("a", DataType::Date32, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![a])?;

        let dfs = schema.clone().to_dfschema()?;
        let props = ExecutionProps::new();

        let lhs = create_physical_expr(dt, &dfs, &schema, &props)?;
        let rhs = create_physical_expr(interval, &dfs, &schema, &props)?;

        let lhs_str = format!("{lhs}");
        let rhs_str = format!("{rhs}");

        let cut = DateTimeIntervalExpr::try_new(lhs, op, rhs, &schema)?;

        assert_eq!(lhs_str, format!("{}", cut.lhs()));
        assert_eq!(op, cut.op().clone());
        assert_eq!(rhs_str, format!("{}", cut.rhs()));

        let res = cut.evaluate(&batch)?;
        Ok(res)
    }
}
