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
use sqlparser::parser::ParserError;

use crate::{DataFusionError, Result, ScalarValue};
use std::result;
use std::str::FromStr;

const SECONDS_PER_HOUR: f64 = 3_600_f64;
const NANOS_PER_SECOND: f64 = 1_000_000_000_f64;

/// Readable file compression type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionTypeVariant {
    /// Gzip-ed file
    GZIP,
    /// Bzip2-ed file
    BZIP2,
    /// Xz-ed file (liblzma)
    XZ,
    /// Uncompressed file
    UNCOMPRESSED,
}

impl FromStr for CompressionTypeVariant {
    type Err = ParserError;

    fn from_str(s: &str) -> result::Result<Self, ParserError> {
        let s = s.to_uppercase();
        match s.as_str() {
            "GZIP" | "GZ" => Ok(Self::GZIP),
            "BZIP2" | "BZ2" => Ok(Self::BZIP2),
            "XZ" => Ok(Self::XZ),
            "" => Ok(Self::UNCOMPRESSED),
            _ => Err(ParserError::ParserError(format!(
                "Unsupported file compression type {s}"
            ))),
        }
    }
}

impl ToString for CompressionTypeVariant {
    fn to_string(&self) -> String {
        match self {
            Self::GZIP => "GZIP",
            Self::BZIP2 => "BZIP2",
            Self::XZ => "XZ",
            Self::UNCOMPRESSED => "",
        }
        .to_string()
    }
}

impl CompressionTypeVariant {
    pub const fn is_compressed(&self) -> bool {
        !matches!(self, &Self::UNCOMPRESSED)
    }
}

#[derive(Clone, Copy)]
#[repr(u16)]
enum IntervalType {
    Century = 0b_00_0000_0001,
    Decade = 0b_00_0000_0010,
    Year = 0b_00_0000_0100,
    Month = 0b_00_0000_1000,
    Week = 0b_00_0001_0000,
    Day = 0b_00_0010_0000,
    Hour = 0b_00_0100_0000,
    Minute = 0b_00_1000_0000,
    Second = 0b_01_0000_0000,
    Millisecond = 0b_10_0000_0000,
}

impl FromStr for IntervalType {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "century" | "centuries" => Ok(Self::Century),
            "decade" | "decades" => Ok(Self::Decade),
            "year" | "years" => Ok(Self::Year),
            "month" | "months" => Ok(Self::Month),
            "week" | "weeks" => Ok(Self::Week),
            "day" | "days" => Ok(Self::Day),
            "hour" | "hours" => Ok(Self::Hour),
            "minute" | "minutes" => Ok(Self::Minute),
            "second" | "seconds" => Ok(Self::Second),
            "millisecond" | "milliseconds" => Ok(Self::Millisecond),
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unknown interval type: {s}"
            ))),
        }
    }
}

/// Parses a string with an interval like `'0.5 MONTH'` to an
/// appropriately typed [`ScalarValue`]
pub fn parse_interval(leading_field: &str, value: &str) -> Result<ScalarValue> {
    // We are storing parts as integers, it's why we need to align parts fractional
    // INTERVAL '0.5 MONTH' = 15 days, INTERVAL '1.5 MONTH' = 1 month 15 days
    // INTERVAL '0.5 DAY' = 12 hours, INTERVAL '1.5 DAY' = 1 day 12 hours
    let align_interval_parts =
        |month_part: f64, mut day_part: f64, mut nanos_part: f64| -> (i64, i64, f64) {
            // Convert fractional month to days, It's not supported by Arrow types, but anyway
            day_part += (month_part - (month_part as i64) as f64) * 30_f64;

            // Convert fractional days to hours
            nanos_part += (day_part - ((day_part as i64) as f64))
                * 24_f64
                * SECONDS_PER_HOUR
                * NANOS_PER_SECOND;

            (month_part as i64, day_part as i64, nanos_part)
        };

    let mut used_interval_types = 0;

    let mut calculate_from_part = |interval_period_str: &str,
                                   interval_type: &str|
     -> Result<(i64, i64, f64)> {
        // @todo It's better to use Decimal in order to protect rounding errors
        // Wait https://github.com/apache/arrow/pull/9232
        let interval_period = match f64::from_str(interval_period_str) {
            Ok(n) => n,
            Err(_) => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported Interval Expression with value {value:?}"
                )));
            }
        };

        if interval_period > (i64::MAX as f64) {
            return Err(DataFusionError::NotImplemented(format!(
                "Interval field value out of range: {value:?}"
            )));
        }

        let it = IntervalType::from_str(interval_type).map_err(|_| {
            DataFusionError::NotImplemented(format!(
                "Invalid input syntax for type interval: {value:?}"
            ))
        })?;

        // Disallow duplicate interval types
        if used_interval_types & (it as u16) != 0 {
            return Err(DataFusionError::SQL(ParserError::ParserError(format!(
                "Invalid input syntax for type interval: {value:?}. Repeated type '{interval_type}'"
            ))));
        } else {
            used_interval_types |= it as u16;
        }

        match it {
            IntervalType::Century => {
                Ok(align_interval_parts(interval_period * 1200_f64, 0.0, 0.0))
            }
            IntervalType::Decade => {
                Ok(align_interval_parts(interval_period * 120_f64, 0.0, 0.0))
            }
            IntervalType::Year => {
                Ok(align_interval_parts(interval_period * 12_f64, 0.0, 0.0))
            }
            IntervalType::Month => Ok(align_interval_parts(interval_period, 0.0, 0.0)),
            IntervalType::Week => {
                Ok(align_interval_parts(0.0, interval_period * 7_f64, 0.0))
            }
            IntervalType::Day => Ok(align_interval_parts(0.0, interval_period, 0.0)),
            IntervalType::Hour => {
                Ok((0, 0, interval_period * SECONDS_PER_HOUR * NANOS_PER_SECOND))
            }
            IntervalType::Minute => {
                Ok((0, 0, interval_period * 60_f64 * NANOS_PER_SECOND))
            }
            IntervalType::Second => Ok((0, 0, interval_period * NANOS_PER_SECOND)),
            IntervalType::Millisecond => Ok((0, 0, interval_period * 1_000_000f64)),
        }
    };

    let mut result_month: i64 = 0;
    let mut result_days: i64 = 0;
    let mut result_nanos: i128 = 0;

    let mut parts = value.split_whitespace();

    loop {
        let interval_period_str = parts.next();
        if interval_period_str.is_none() {
            break;
        }

        let unit = parts.next().unwrap_or(leading_field);

        let (diff_month, diff_days, diff_nanos) =
            calculate_from_part(interval_period_str.unwrap(), unit)?;

        result_month += diff_month;

        if result_month > (i32::MAX as i64) {
            return Err(DataFusionError::NotImplemented(format!(
                "Interval field value out of range: {value:?}"
            )));
        }

        result_days += diff_days;

        if result_days > (i32::MAX as i64) {
            return Err(DataFusionError::NotImplemented(format!(
                "Interval field value out of range: {value:?}"
            )));
        }

        result_nanos += diff_nanos as i128;

        if result_nanos > (i64::MAX as i128) {
            return Err(DataFusionError::NotImplemented(format!(
                "Interval field value out of range: {value:?}"
            )));
        }
    }

    // Interval is tricky thing
    // 1 day is not 24 hours because timezones, 1 year != 365/364! 30 days != 1 month
    // The true way to store and calculate intervals is to store it as it defined
    // It's why we there are 3 different interval types in Arrow

    // If have a unit smaller than milliseconds then must use IntervalMonthDayNano
    if (result_nanos % 1_000_000 != 0)
        || (result_month != 0 && (result_days != 0 || result_nanos != 0))
    {
        let result: i128 = ((result_month as i128) << 96)
            // ensure discard high 32 bits of result_days before casting to i128
            | (((result_days & u32::MAX as i64) as i128) << 64)
            // ensure discard high 64 bits of result_nanos
            | (result_nanos & u64::MAX as i128);

        return Ok(ScalarValue::IntervalMonthDayNano(Some(result)));
    }

    // Month interval
    if result_month != 0 {
        return Ok(ScalarValue::IntervalYearMonth(Some(result_month as i32)));
    }

    // IntervalMonthDayNano uses nanos, but IntervalDayTime uses millis
    let result: i64 =
        // ensure discard high 32 bits of milliseconds
        (result_days << 32) | ((result_nanos as i64 / 1_000_000) & (u32::MAX as i64));
    Ok(ScalarValue::IntervalDayTime(Some(result)))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::assert_contains;

    const MILLIS_PER_SECOND: f64 = 1_000_f64;

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

        assert_eq!(
            parse_interval("months", "3 year -1 month").unwrap(),
            ScalarValue::new_interval_ym(3, -1)
        );

        assert_eq!(
            parse_interval("months", "-3 year -1 month").unwrap(),
            ScalarValue::new_interval_ym(-3, -1)
        );

        assert_eq!(
            parse_interval("months", "-3 year 1 month").unwrap(),
            ScalarValue::new_interval_ym(-3, 1)
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
            ScalarValue::new_interval_dt(7, 5 * 60 * MILLIS_PER_SECOND as i32)
        );

        assert_eq!(
            parse_interval("months", "7 days -5 minutes").unwrap(),
            ScalarValue::new_interval_dt(7, -5 * 60 * MILLIS_PER_SECOND as i32)
        );

        assert_eq!(
            parse_interval("months", "-7 days 5 hours").unwrap(),
            ScalarValue::new_interval_dt(-7, 5 * 60 * 60 * MILLIS_PER_SECOND as i32)
        );

        assert_eq!(
            parse_interval("months", "-7 days -5 hours -5 minutes -5 seconds").unwrap(),
            ScalarValue::new_interval_dt(
                -7,
                -(5 * 60 * 60 + 5 * 60 + 5) * MILLIS_PER_SECOND as i32
            )
        );
    }

    #[test]
    fn test_mdn() {
        assert_eq!(
            parse_interval("months", "1 year 25 millisecond").unwrap(),
            ScalarValue::new_interval_mdn(12, 0, 25 * 1_000_000)
        );

        assert_eq!(
            parse_interval("months", "1 year 1 day 0.000000001 seconds").unwrap(),
            ScalarValue::new_interval_mdn(12, 1, 1)
        );

        assert_eq!(
            parse_interval("months", "1 year 1 day 0.1 milliseconds").unwrap(),
            ScalarValue::new_interval_mdn(12, 1, 1_00 * 1_000)
        );

        assert_eq!(
            parse_interval("months", "1 month -1 second").unwrap(),
            ScalarValue::new_interval_mdn(1, 0, -1_000_000_000)
        );

        assert_eq!(
            parse_interval("months", "-1 year -1 month -1 week -1 day -1 hour -1 minute -1 second -1.11 millisecond").unwrap(),
            ScalarValue::new_interval_mdn(-13, -8, -(60 * 60 + 60 + 1) * NANOS_PER_SECOND as i64 - 1_110_000)
        );
    }

    #[test]
    fn test_duplicate_interval_type() {
        let err = parse_interval("months", "1 month 1 second 1 second")
            .expect_err("parsing interval should have failed");
        assert_eq!(
            r#"SQL(ParserError("Invalid input syntax for type interval: \"1 month 1 second 1 second\". Repeated type 'second'"))"#,
            format!("{err:?}")
        );
    }
}
