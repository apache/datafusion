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

use crate::delta::shift_months;
use crate::Result;
use crate::{DataFusionError, ScalarValue};
use arrow::datatypes::{IntervalDayTimeType, IntervalMonthDayNanoType};
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime};
use std::ops::{Add, Sub};

pub fn evaluate_scalar(
    operand: ScalarValue,
    sign: i32,
    scalar: &ScalarValue,
) -> Result<ScalarValue> {
    let res = match operand {
        ScalarValue::Date32(Some(days)) => {
            let value = date32_add(days, scalar, sign)?;
            ScalarValue::Date32(Some(value))
        }
        ScalarValue::Date64(Some(ms)) => {
            let value = date64_add(ms, scalar, sign)?;
            ScalarValue::Date64(Some(value))
        }
        ScalarValue::TimestampSecond(Some(ts_s), zone) => {
            let value = seconds_add(ts_s, scalar, sign)?;
            ScalarValue::TimestampSecond(Some(value), zone)
        }
        ScalarValue::TimestampMillisecond(Some(ts_ms), zone) => {
            let value = milliseconds_add(ts_ms, scalar, sign)?;
            ScalarValue::TimestampMillisecond(Some(value), zone)
        }
        ScalarValue::TimestampMicrosecond(Some(ts_us), zone) => {
            let value = microseconds_add(ts_us, scalar, sign)?;
            ScalarValue::TimestampMicrosecond(Some(value), zone)
        }
        ScalarValue::TimestampNanosecond(Some(ts_ns), zone) => {
            let value = nanoseconds_add(ts_ns, scalar, sign)?;
            ScalarValue::TimestampNanosecond(Some(value), zone)
        }
        _ => Err(DataFusionError::Execution(format!(
            "Invalid lhs type {} for DateIntervalExpr",
            operand.get_datatype()
        )))?,
    };
    Ok(res)
}

#[inline]
pub fn date32_add(days: i32, scalar: &ScalarValue, sign: i32) -> Result<i32> {
    let epoch = NaiveDate::from_ymd(1970, 1, 1);
    let prior = epoch.add(Duration::days(days as i64));
    let posterior = do_date_math(prior, scalar, sign)?;
    Ok(posterior.sub(epoch).num_days() as i32)
}

#[inline]
pub fn date64_add(ms: i64, scalar: &ScalarValue, sign: i32) -> Result<i64> {
    let epoch = NaiveDate::from_ymd(1970, 1, 1);
    let prior = epoch.add(Duration::milliseconds(ms));
    let posterior = do_date_math(prior, scalar, sign)?;
    Ok(posterior.sub(epoch).num_milliseconds())
}

#[inline]
pub fn seconds_add(ts_s: i64, scalar: &ScalarValue, sign: i32) -> Result<i64> {
    Ok(do_date_time_math(ts_s, 0, scalar, sign)?.timestamp())
}

#[inline]
pub fn milliseconds_add(ts_ms: i64, scalar: &ScalarValue, sign: i32) -> Result<i64> {
    let secs = ts_ms / 1000;
    let nsecs = ((ts_ms % 1000) * 1_000_000) as u32;
    Ok(do_date_time_math(secs, nsecs, scalar, sign)?.timestamp_millis())
}

#[inline]
pub fn microseconds_add(ts_us: i64, scalar: &ScalarValue, sign: i32) -> Result<i64> {
    let secs = ts_us / 1_000_000;
    let nsecs = ((ts_us % 1_000_000) * 1000) as u32;
    Ok(do_date_time_math(secs, nsecs, scalar, sign)?.timestamp_nanos() / 1000)
}

#[inline]
pub fn nanoseconds_add(ts_ns: i64, scalar: &ScalarValue, sign: i32) -> Result<i64> {
    let secs = ts_ns / 1_000_000_000;
    let nsecs = (ts_ns % 1_000_000_000) as u32;
    Ok(do_date_time_math(secs, nsecs, scalar, sign)?.timestamp_nanos())
}

#[inline]
fn do_date_time_math(
    secs: i64,
    nsecs: u32,
    scalar: &ScalarValue,
    sign: i32,
) -> Result<NaiveDateTime> {
    let prior = NaiveDateTime::from_timestamp(secs, nsecs);
    do_date_math(prior, scalar, sign)
}

fn do_date_math<D>(prior: D, scalar: &ScalarValue, sign: i32) -> Result<D>
where
    D: Datelike + Add<Duration, Output = D>,
{
    Ok(match scalar {
        ScalarValue::IntervalDayTime(Some(i)) => add_day_time(prior, *i, sign),
        ScalarValue::IntervalYearMonth(Some(i)) => shift_months(prior, *i * sign),
        ScalarValue::IntervalMonthDayNano(Some(i)) => add_m_d_nano(prior, *i, sign),
        other => Err(DataFusionError::Execution(format!(
            "DateIntervalExpr does not support non-interval type {:?}",
            other
        )))?,
    })
}

// Can remove once https://github.com/apache/arrow-rs/pull/2031 is released
fn add_m_d_nano<D>(prior: D, interval: i128, sign: i32) -> D
where
    D: Datelike + Add<Duration, Output = D>,
{
    // let interval = interval as u128;
    // let nanos = (interval >> 64) as i64 * sign as i64;
    // let days = (interval >> 32) as i32 * sign;
    // let months = interval as i32 * sign;
    let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(interval);
    let months = months * sign;
    let days = days * sign;
    let nanos = nanos * sign as i64;
    let a = shift_months(prior, months);
    let b = a.add(Duration::days(days as i64));
    b.add(Duration::nanoseconds(nanos))
}

// Can remove once https://github.com/apache/arrow-rs/pull/2031 is released
fn add_day_time<D>(prior: D, interval: i64, sign: i32) -> D
where
    D: Datelike + Add<Duration, Output = D>,
{
    let (days, ms) = IntervalDayTimeType::to_parts(interval);
    let days = days * sign;
    let ms = ms * sign;
    let intermediate = prior.add(Duration::days(days as i64));
    intermediate.add(Duration::milliseconds(ms as i64))
}
