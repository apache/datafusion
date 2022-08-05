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

//! Interval parsing logic
use datafusion_common::{DataFusionError, Result, ScalarValue};
use std::str::FromStr;

const SECONDS_PER_HOUR: f32 = 3_600_f32;
const MILLIS_PER_SECOND: f32 = 1_000_f32;

/// Parses a string with an interval like `'0.5 MONTH'` to an
/// appropriately typed [`ScalarValue`]
pub(crate) fn parse_interval(leading_field: &str, value: &str) -> Result<ScalarValue> {
    // We are storing parts as integers, it's why we need to align parts fractional
    // INTERVAL '0.5 MONTH' = 15 days, INTERVAL '1.5 MONTH' = 1 month 15 days
    // INTERVAL '0.5 DAY' = 12 hours, INTERVAL '1.5 DAY' = 1 day 12 hours
    let align_interval_parts =
        |month_part: f32, mut day_part: f32, mut milles_part: f32| -> (i32, i32, f32) {
            // Convert fractional month to days, It's not supported by Arrow types, but anyway
            day_part += (month_part - (month_part as i32) as f32) * 30_f32;

            // Convert fractional days to hours
            milles_part += (day_part - ((day_part as i32) as f32))
                * 24_f32
                * SECONDS_PER_HOUR
                * MILLIS_PER_SECOND;

            (month_part as i32, day_part as i32, milles_part)
        };

    let calculate_from_part =
        |interval_period_str: &str, interval_type: &str| -> Result<(i32, i32, f32)> {
            // @todo It's better to use Decimal in order to protect rounding errors
            // Wait https://github.com/apache/arrow/pull/9232
            let interval_period = match f32::from_str(interval_period_str) {
                Ok(n) => n,
                Err(_) => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Unsupported Interval Expression with value {:?}",
                        value
                    )));
                }
            };

            if interval_period > (i32::MAX as f32) {
                return Err(DataFusionError::NotImplemented(format!(
                    "Interval field value out of range: {:?}",
                    value
                )));
            }

            match interval_type.to_lowercase().as_str() {
                "century" | "centuries" => {
                    Ok(align_interval_parts(interval_period * 1200_f32, 0.0, 0.0))
                }
                "decade" | "decades" => {
                    Ok(align_interval_parts(interval_period * 120_f32, 0.0, 0.0))
                }
                "year" | "years" => {
                    Ok(align_interval_parts(interval_period * 12_f32, 0.0, 0.0))
                }
                "month" | "months" => Ok(align_interval_parts(interval_period, 0.0, 0.0)),
                "week" | "weeks" => {
                    Ok(align_interval_parts(0.0, interval_period * 7_f32, 0.0))
                }
                "day" | "days" => Ok(align_interval_parts(0.0, interval_period, 0.0)),
                "hour" | "hours" => {
                    Ok((0, 0, interval_period * SECONDS_PER_HOUR * MILLIS_PER_SECOND))
                }
                "minute" | "minutes" => {
                    Ok((0, 0, interval_period * 60_f32 * MILLIS_PER_SECOND))
                }
                "second" | "seconds" => Ok((0, 0, interval_period * MILLIS_PER_SECOND)),
                "millisecond" | "milliseconds" => Ok((0, 0, interval_period)),
                _ => Err(DataFusionError::NotImplemented(format!(
                    "Invalid input syntax for type interval: {:?}",
                    value
                ))),
            }
        };

    let mut result_month: i64 = 0;
    let mut result_days: i64 = 0;
    let mut result_millis: i64 = 0;

    let mut parts = value.split_whitespace();

    loop {
        let interval_period_str = parts.next();
        if interval_period_str.is_none() {
            break;
        }

        let unit = parts.next().unwrap_or(leading_field);

        let (diff_month, diff_days, diff_millis) =
            calculate_from_part(interval_period_str.unwrap(), unit)?;

        result_month += diff_month as i64;

        if result_month > (i32::MAX as i64) {
            return Err(DataFusionError::NotImplemented(format!(
                "Interval field value out of range: {:?}",
                value
            )));
        }

        result_days += diff_days as i64;

        if result_days > (i32::MAX as i64) {
            return Err(DataFusionError::NotImplemented(format!(
                "Interval field value out of range: {:?}",
                value
            )));
        }

        result_millis += diff_millis as i64;

        if result_millis > (i32::MAX as i64) {
            return Err(DataFusionError::NotImplemented(format!(
                "Interval field value out of range: {:?}",
                value
            )));
        }
    }

    // Interval is tricky thing
    // 1 day is not 24 hours because timezones, 1 year != 365/364! 30 days != 1 month
    // The true way to store and calculate intervals is to store it as it defined
    // It's why we there are 3 different interval types in Arrow
    if result_month != 0 && (result_days != 0 || result_millis != 0) {
        let result: i128 = ((result_month as i128) << 96)
                | ((result_days as i128) << 64)
                // IntervalMonthDayNano uses nanos, but IntervalDayTime uses milles
                | ((result_millis * 1_000_000_i64) as i128);

        return Ok(ScalarValue::IntervalMonthDayNano(Some(result)));
    }

    // Month interval
    if result_month != 0 {
        return Ok(ScalarValue::IntervalYearMonth(Some(result_month as i32)));
    }

    let result: i64 = (result_days << 32) | result_millis;
    Ok(ScalarValue::IntervalDayTime(Some(result)))
}

#[cfg(test)]
mod test {
    use crate::assert_contains;

    use super::*;

    #[test]
    fn test_parse_ym() {
        assert_eq!(
            parse_interval("months", "1 month").unwrap(),
            ScalarValue::new_interval_ym(0, 1)
        );

        assert_eq!(
            parse_interval("months", "2 month").unwrap(),
            ScalarValue::new_interval_ym(0, 2)
        );

        assert_eq!(
            parse_interval("months", "3 year 1 month").unwrap(),
            ScalarValue::new_interval_ym(3, 1)
        );

        assert_contains!(
            parse_interval("months", "1 centurys 1 month")
                .unwrap_err()
                .to_string(),
            r#"Invalid input syntax for type interval: "1 centurys 1 month""#
        );
    }

    #[test]
    fn test_dt() {
        assert_eq!(
            parse_interval("months", "5 days").unwrap(),
            ScalarValue::new_interval_dt(5, 0)
        );

        assert_eq!(
            parse_interval("months", "7 days 3 hours").unwrap(),
            ScalarValue::new_interval_dt(
                7,
                (3.0 * SECONDS_PER_HOUR * MILLIS_PER_SECOND) as i32
            )
        );

        assert_eq!(
            parse_interval("months", "7 days 5 minutes").unwrap(),
            ScalarValue::new_interval_dt(7, (5.0 * 60.0 * MILLIS_PER_SECOND) as i32)
        );
    }

    #[test]
    // https://github.com/apache/arrow-rs/pull/2235
    #[ignore]
    fn test_mdn() {
        // these should be the same, I think
        assert_eq!(
            parse_interval("months", "1 year 25 millisecond").unwrap(),
            ScalarValue::new_interval_mdn(12, 0, 25 * 1_000_000)
        );
    }
}
