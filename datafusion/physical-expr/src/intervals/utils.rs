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

//! Utility functions for the interval arithmetic library

use arrow::array::types::{IntervalDayTime, IntervalMonthDayNano};
use datafusion_common::{internal_err, Result, ScalarValue};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::Operator;

// This function returns the inverse operator of the given operator.
pub fn get_inverse_op(op: Operator) -> Result<Operator> {
    match op {
        Operator::Plus => Ok(Operator::Minus),
        Operator::Minus => Ok(Operator::Plus),
        Operator::Multiply => Ok(Operator::Divide),
        Operator::Divide => Ok(Operator::Multiply),
        _ => internal_err!("Interval arithmetic does not support the operator {}", op),
    }
}

/// Converts an [`Interval`] of time intervals to one of `Duration`s, if applicable. Otherwise, returns [`None`].
pub fn convert_interval_type_to_duration(interval: &Interval) -> Option<Interval> {
    if let (Some(lower), Some(upper)) = (
        convert_interval_bound_to_duration(interval.lower()),
        convert_interval_bound_to_duration(interval.upper()),
    ) {
        Interval::try_new(lower, upper).ok()
    } else {
        None
    }
}

/// Converts an [`ScalarValue`] containing a time interval to one containing a `Duration`, if applicable. Otherwise, returns [`None`].
fn convert_interval_bound_to_duration(
    interval_bound: &ScalarValue,
) -> Option<ScalarValue> {
    match interval_bound {
        ScalarValue::IntervalMonthDayNano(Some(mdn)) => interval_mdn_to_duration_ns(mdn)
            .ok()
            .map(|duration| ScalarValue::DurationNanosecond(Some(duration))),
        ScalarValue::IntervalDayTime(Some(dt)) => interval_dt_to_duration_ms(dt)
            .ok()
            .map(|duration| ScalarValue::DurationMillisecond(Some(duration))),
        _ => None,
    }
}

/// Converts an [`Interval`] of `Duration`s to one of time intervals, if applicable. Otherwise, returns [`None`].
pub fn convert_duration_type_to_interval(interval: &Interval) -> Option<Interval> {
    if let (Some(lower), Some(upper)) = (
        convert_duration_bound_to_interval(interval.lower()),
        convert_duration_bound_to_interval(interval.upper()),
    ) {
        Interval::try_new(lower, upper).ok()
    } else {
        None
    }
}

/// Converts a [`ScalarValue`] containing a `Duration` to one containing a time interval, if applicable. Otherwise, returns [`None`].
fn convert_duration_bound_to_interval(
    interval_bound: &ScalarValue,
) -> Option<ScalarValue> {
    match interval_bound {
        ScalarValue::DurationNanosecond(Some(duration)) => {
            Some(ScalarValue::new_interval_mdn(0, 0, *duration))
        }
        ScalarValue::DurationMicrosecond(Some(duration)) => {
            Some(ScalarValue::new_interval_mdn(0, 0, *duration * 1000))
        }
        ScalarValue::DurationMillisecond(Some(duration)) => {
            Some(ScalarValue::new_interval_dt(0, *duration as i32))
        }
        ScalarValue::DurationSecond(Some(duration)) => {
            Some(ScalarValue::new_interval_dt(0, *duration as i32 * 1000))
        }
        _ => None,
    }
}

/// If both the month and day fields of [`ScalarValue::IntervalMonthDayNano`] are zero, this function returns the nanoseconds part.
/// Otherwise, it returns an error.
fn interval_mdn_to_duration_ns(mdn: &IntervalMonthDayNano) -> Result<i64> {
    if mdn.months == 0 && mdn.days == 0 {
        Ok(mdn.nanoseconds)
    } else {
        internal_err!(
            "The interval cannot have a non-zero month or day value for duration convertibility"
        )
    }
}

/// If the day field of the [`ScalarValue::IntervalDayTime`] is zero, this function returns the milliseconds part.
/// Otherwise, it returns an error.
fn interval_dt_to_duration_ms(dt: &IntervalDayTime) -> Result<i64> {
    if dt.days == 0 {
        // Safe to cast i32 to i64
        Ok(dt.milliseconds as i64)
    } else {
        internal_err!(
            "The interval cannot have a non-zero day value for duration convertibility"
        )
    }
}
