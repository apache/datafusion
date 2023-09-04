// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Utility functions for the interval arithmetic library

use std::sync::Arc;

use arrow_schema::DataType;
use datafusion_common::{
    scalar::{MILLISECS_IN_ONE_DAY, NANOSECS_IN_ONE_DAY},
    ScalarValue,
};
use datafusion_expr::Operator;

use crate::{
    expressions::{
        interval_dt_to_duration_ms, interval_mdn_to_duration_ns, BinaryExpr, CastExpr,
        Column, Literal,
    },
    PhysicalExpr,
};

use super::{Interval, IntervalBound};

/// Indicates whether interval arithmetic is supported for the given expression.
/// Currently, we do not support all [`PhysicalExpr`]s for interval calculations.
/// We do not support every type of [`Operator`]s either. Over time, this check
/// will relax as more types of `PhysicalExpr`s and `Operator`s are supported.
/// Currently, [`CastExpr`], [`BinaryExpr`], [`Column`] and [`Literal`] are supported.
pub fn check_support(expr: &Arc<dyn PhysicalExpr>) -> bool {
    let expr_any = expr.as_any();
    let expr_supported = if let Some(binary_expr) = expr_any.downcast_ref::<BinaryExpr>()
    {
        is_operator_supported(binary_expr.op())
    } else {
        expr_any.is::<Column>() || expr_any.is::<Literal>() || expr_any.is::<CastExpr>()
    };
    expr_supported && expr.children().iter().all(check_support)
}

// This function returns the inverse operator of the given operator.
pub fn get_inverse_op(op: Operator) -> Operator {
    match op {
        Operator::Plus => Operator::Minus,
        Operator::Minus => Operator::Plus,
        _ => unreachable!(),
    }
}

/// Indicates whether interval arithmetic is supported for the given operator.
pub fn is_operator_supported(op: &Operator) -> bool {
    matches!(
        op,
        &Operator::Plus
            | &Operator::Minus
            | &Operator::And
            | &Operator::Gt
            | &Operator::GtEq
            | &Operator::Lt
            | &Operator::LtEq
            | &Operator::Eq
    )
}

/// Indicates whether interval arithmetic is supported for the given data type.
pub fn is_datatype_supported(data_type: &DataType) -> bool {
    matches!(
        data_type,
        &DataType::Int64
            | &DataType::Int32
            | &DataType::Int16
            | &DataType::Int8
            | &DataType::UInt64
            | &DataType::UInt32
            | &DataType::UInt16
            | &DataType::UInt8
            | &DataType::Float64
            | &DataType::Float32
    )
}

/// Converts the [`Interval`] of time interval to [`Interval`] of duration, if it is applicable, or returns [`None`].
pub fn convert_interval_type_to_duration(interval: &Interval) -> Option<Interval> {
    if let (Some(lower), Some(upper)) = (
        convert_interval_bound_to_duration(&interval.lower),
        convert_interval_bound_to_duration(&interval.upper),
    ) {
        Some(Interval::new(lower, upper))
    } else {
        None
    }
}

/// Converts an [`IntervalBound`] containing a time interval to one containing a `Duration`, if applicable. Otherwise, returns [`None`].
fn convert_interval_bound_to_duration(
    interval_bound: &IntervalBound,
) -> Option<IntervalBound> {
    match interval_bound.value {
        ScalarValue::IntervalMonthDayNano(Some(mdn)) => {
            interval_mdn_to_duration_ns(&mdn).ok().map(|duration| {
                IntervalBound::new(
                    ScalarValue::DurationNanosecond(Some(duration)),
                    interval_bound.open,
                )
            })
        }
        ScalarValue::IntervalDayTime(Some(dt)) => {
            interval_dt_to_duration_ms(&dt).ok().map(|duration| {
                IntervalBound::new(
                    ScalarValue::DurationMillisecond(Some(duration)),
                    interval_bound.open,
                )
            })
        }
        _ => None,
    }
}

/// Converts the [`Interval`] of duration to [`Interval`] of time interval, if it is applicable, or returns [`None`].
pub fn convert_duration_type_to_interval(interval: &Interval) -> Option<Interval> {
    if let (Some(lower), Some(upper)) = (
        convert_duration_bound_to_interval(&interval.lower),
        convert_duration_bound_to_interval(&interval.upper),
    ) {
        Some(Interval::new(lower, upper))
    } else {
        None
    }
}

/// Converts the [`IntervalBound`] of duration to [`IntervalBound`] of time interval, if it is applicable, or returns [`None`].
fn convert_duration_bound_to_interval(
    interval_bound: &IntervalBound,
) -> Option<IntervalBound> {
    match interval_bound.value {
        ScalarValue::DurationNanosecond(Some(duration)) => Some(IntervalBound::new(
            ScalarValue::new_interval_mdn(
                0,
                (duration / NANOSECS_IN_ONE_DAY) as i32,
                duration % NANOSECS_IN_ONE_DAY,
            ),
            interval_bound.open,
        )),
        ScalarValue::DurationMillisecond(Some(duration)) => Some(IntervalBound::new(
            ScalarValue::new_interval_dt(
                (duration / MILLISECS_IN_ONE_DAY) as i32,
                (duration % MILLISECS_IN_ONE_DAY) as i32,
            ),
            interval_bound.open,
        )),
        _ => None,
    }
}
