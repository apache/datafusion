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

use arrow::array::timezone::Tz;
use arrow::datatypes::{ArrowTimestampType, TimeUnit};
use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, Utc};
use datafusion_common::{Result, exec_datafusion_err, internal_datafusion_err};
use datafusion_functions::datetime::to_local_time::adjust_to_local_time;

/// A timezone resolved using Spark's `getZoneId` spelling rules.
///
/// Fixed offsets are kept as seconds to preserve Spark's second-resolution offsets.
/// Region IDs use Arrow's Chrono-backed timezone database.
/// Java's legacy SystemV zones require their own rules because that database omits them.
#[derive(Debug, Clone, Copy)]
pub(super) enum SparkTimeZone {
    FixedOffset(i32),
    Region(Tz),
    SystemV(i32),
}

impl SparkTimeZone {
    pub(super) fn parse(timezone: &str) -> Result<Self> {
        // These are the exact aliases in java.time.ZoneId.SHORT_IDS. The regional
        // aliases retain their daylight saving rules; EST, HST, and MST are fixed.
        let timezone = match timezone {
            "Z" => return Ok(Self::FixedOffset(0)),
            // The JDK explicitly excludes this legacy IANA alias.
            "ROC" => return Err(exec_datafusion_err!("Unknown timezone '{timezone}'")),
            "EST" => return Ok(Self::FixedOffset(-5 * 3600)),
            "HST" => return Ok(Self::FixedOffset(-10 * 3600)),
            "MST" => return Ok(Self::FixedOffset(-7 * 3600)),
            "ACT" => "Australia/Darwin",
            "AET" => "Australia/Sydney",
            "AGT" => "America/Argentina/Buenos_Aires",
            "ART" => "Africa/Cairo",
            "AST" => "America/Anchorage",
            "BET" => "America/Sao_Paulo",
            "BST" => "Asia/Dhaka",
            "CAT" => "Africa/Harare",
            "CNT" => "America/St_Johns",
            "CST" => "America/Chicago",
            "CTT" => "Asia/Shanghai",
            "EAT" => "Africa/Addis_Ababa",
            "ECT" => "Europe/Paris",
            "IET" => "America/Indiana/Indianapolis",
            "IST" => "Asia/Kolkata",
            "JST" => "Asia/Tokyo",
            "MIT" => "Pacific/Apia",
            "NET" => "Asia/Yerevan",
            "NST" => "Pacific/Auckland",
            "PLT" => "Asia/Karachi",
            "PNT" => "America/Phoenix",
            "PRT" => "America/Puerto_Rico",
            "PST" => "America/Los_Angeles",
            "SST" => "Pacific/Guadalcanal",
            "VST" => "Asia/Ho_Chi_Minh",
            "SystemV/AST4" => return Ok(Self::FixedOffset(-4 * 3600)),
            "SystemV/EST5" => return Ok(Self::FixedOffset(-5 * 3600)),
            "SystemV/CST6" => return Ok(Self::FixedOffset(-6 * 3600)),
            "SystemV/MST7" => return Ok(Self::FixedOffset(-7 * 3600)),
            "SystemV/PST8" => return Ok(Self::FixedOffset(-8 * 3600)),
            "SystemV/YST9" => return Ok(Self::FixedOffset(-9 * 3600)),
            "SystemV/HST10" => return Ok(Self::FixedOffset(-10 * 3600)),
            "SystemV/AST4ADT" => return Ok(Self::SystemV(-4 * 3600)),
            "SystemV/EST5EDT" => return Ok(Self::SystemV(-5 * 3600)),
            "SystemV/CST6CDT" => return Ok(Self::SystemV(-6 * 3600)),
            "SystemV/MST7MDT" => return Ok(Self::SystemV(-7 * 3600)),
            "SystemV/PST8PDT" => return Ok(Self::SystemV(-8 * 3600)),
            "SystemV/YST9YDT" => return Ok(Self::SystemV(-9 * 3600)),
            _ => timezone,
        };

        let offset = if timezone.starts_with(['+', '-']) {
            Some(timezone)
        } else {
            ["UTC", "GMT", "UT"].into_iter().find_map(|prefix| {
                timezone
                    .strip_prefix(prefix)
                    .filter(|suffix| suffix.is_empty() || suffix.starts_with(['+', '-']))
            })
        };
        if let Some(offset) = offset {
            if offset.is_empty() {
                return Ok(Self::FixedOffset(0));
            }
            return parse_offset(offset).map(Self::FixedOffset).ok_or_else(|| {
                exec_datafusion_err!("Invalid timezone offset '{offset}'")
            });
        }

        Ok(Self::Region(timezone.parse()?))
    }

    pub(super) fn adjust_to_local_time<T: ArrowTimestampType>(
        self,
        ts: i64,
    ) -> Result<i64> {
        match self {
            Self::Region(timezone) => adjust_to_local_time::<T>(ts, timezone),
            Self::FixedOffset(seconds) => adjust_by_offset::<T>(ts, seconds),
            Self::SystemV(standard_offset) => {
                adjust_by_offset::<T>(ts, system_v_offset::<T>(ts, standard_offset)?)
            }
        }
    }
}

/// Parses Java's signed offsets after Spark's single-digit hour/minute normalization.
///
/// Spark permits one-digit hours before a colon, and one-digit minutes only when
/// minutes are the final component. Java also accepts compact HHMM and HHMMSS forms.
fn parse_offset(offset: &str) -> Option<i32> {
    let sign = match offset.as_bytes().first()? {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let value = &offset[1..];
    let (hours, minutes, seconds) = if let Some((hours, rest)) = value.split_once(':') {
        let hours = parse_digits(hours, 1, 2)?;
        if let Some((minutes, seconds)) = rest.split_once(':') {
            (
                hours,
                parse_digits(minutes, 2, 2)?,
                parse_digits(seconds, 2, 2)?,
            )
        } else {
            (hours, parse_digits(rest, 1, 2)?, 0)
        }
    } else {
        // Check ASCII before byte slicing so malformed Unicode inputs cannot panic.
        if !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        match value.len() {
            1 | 2 => (parse_digits(value, 1, 2)?, 0, 0),
            4 => (
                parse_digits(&value[..2], 2, 2)?,
                parse_digits(&value[2..], 2, 2)?,
                0,
            ),
            6 => (
                parse_digits(&value[..2], 2, 2)?,
                parse_digits(&value[2..4], 2, 2)?,
                parse_digits(&value[4..], 2, 2)?,
            ),
            _ => return None,
        }
    };
    if hours > 18
        || minutes > 59
        || seconds > 59
        || (hours == 18 && (minutes != 0 || seconds != 0))
    {
        return None;
    }
    Some(sign * (hours * 3600 + minutes * 60 + seconds))
}

fn parse_digits(value: &str, min_len: usize, max_len: usize) -> Option<i32> {
    if !(min_len..=max_len).contains(&value.len())
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    value.parse().ok()
}

fn units_per_second<T: ArrowTimestampType>() -> i64 {
    match T::UNIT {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

fn adjust_by_offset<T: ArrowTimestampType>(ts: i64, offset_seconds: i32) -> Result<i64> {
    let timezone = FixedOffset::east_opt(offset_seconds).ok_or_else(|| {
        internal_datafusion_err!("Spark timezone offsets must be within 18 hours")
    })?;
    adjust_to_local_time::<T>(ts, timezone)
}

/// Java's SystemV daylight zones use the same calendar with different standard offsets.
/// The JDK starts these transitions in 1900. Most years change on the last Sundays of
/// April and October; 1974 and 1975 have exceptional start/end dates.
/// The rule definitions are in OpenJDK's `make/data/tzdata/jdk11_backward`.
fn system_v_offset<T: ArrowTimestampType>(ts: i64, standard_offset: i32) -> Result<i32> {
    let seconds = ts.div_euclid(units_per_second::<T>());
    let year = DateTime::<Utc>::from_timestamp(seconds, 0)
        .ok_or_else(|| {
            exec_datafusion_err!("Timestamp is outside Chrono's calendar range")
        })?
        .year();
    if year < 1900 {
        return Ok(standard_offset);
    }

    let start = match year {
        1974 => NaiveDate::from_ymd_opt(year, 1, 6),
        1975 => NaiveDate::from_ymd_opt(year, 2, 23),
        _ => last_sunday(year, 4, 30),
    };
    let end = if year == 1974 {
        last_sunday(year, 11, 30)
    } else {
        last_sunday(year, 10, 31)
    };
    let transition = |date: Option<NaiveDate>, offset_before: i32| {
        date.and_then(|date| date.and_hms_opt(2, 0, 0))
            .map(|local| local.and_utc().timestamp() - i64::from(offset_before))
            .ok_or_else(|| {
                exec_datafusion_err!("Timestamp is outside Chrono's calendar range")
            })
    };
    let start = transition(start, standard_offset)?;
    let end = transition(end, standard_offset + 3600)?;
    Ok(if (start..end).contains(&seconds) {
        standard_offset + 3600
    } else {
        standard_offset
    })
}

fn last_sunday(year: i32, month: u32, last_day: u32) -> Option<NaiveDate> {
    let date = NaiveDate::from_ymd_opt(year, month, last_day)?;
    date.with_day(last_day - date.weekday().num_days_from_sunday())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::TimestampMicrosecondType;

    fn shift(timezone: &str, timestamp: i64) -> i64 {
        SparkTimeZone::parse(timezone)
            .unwrap()
            .adjust_to_local_time::<TimestampMicrosecondType>(timestamp)
            .unwrap()
    }

    #[test]
    fn spark_offset_spellings_preserve_sign_and_seconds() {
        let offsets = [
            ("+0", 0),
            ("-00", 0),
            ("+1", 3600),
            ("-01", -3600),
            ("+0102", 3720),
            ("-01:02", -3720),
            ("+1:02", 3720),
            ("+01:2", 3720),
            ("+1:2", 3720),
            ("+010203", 3723),
            ("-01:02:03", -3723),
            ("+1:02:03", 3723),
            ("+00:00:01", 1),
            ("-000001", -1),
            ("+18:00", 64800),
            ("-180000", -64800),
        ];
        for prefix in ["", "UTC", "GMT", "UT"] {
            for (offset, seconds) in offsets {
                let timezone = format!("{prefix}{offset}");
                assert_eq!(
                    shift(&timezone, 123_456),
                    123_456 + i64::from(seconds) * 1_000_000,
                    "{timezone}"
                );
            }
        }
        for timezone in ["Z", "UTC", "GMT", "UT", "GMT0", "Etc/UTC"] {
            assert_eq!(shift(timezone, -123_456), -123_456, "{timezone}");
        }
        assert_eq!(shift("GMT+1", 0), 3_600_000_000);
        assert_eq!(shift("Etc/GMT+1", 0), -3_600_000_000);
    }

    #[test]
    fn malformed_offsets_and_unknown_ids_are_rejected() {
        for prefix in ["", "UTC", "GMT", "UT"] {
            for offset in [
                "+",
                "-",
                "+19",
                "-18:00:01",
                "+18:01",
                "+01:60",
                "+01:99",
                "+01:00:60",
                "+1:2:03",
                "+01:02:3",
                "+123",
                "+12345",
                "+001:00",
                "+01:",
                "+:01",
                "+01::00",
                "+01:00:00:00",
                "+01:00 ",
                "+０1",
                "+01:٠0",
                "++01",
                "--01",
            ] {
                let timezone = format!("{prefix}{offset}");
                assert!(SparkTimeZone::parse(&timezone).is_err(), "{timezone}");
            }
        }
        for timezone in [
            "",
            " ",
            "utc",
            "pst",
            "PDT",
            "ROC",
            "America/Unknown",
            "SystemV/PST9PDT",
            " UTC",
        ] {
            assert!(SparkTimeZone::parse(timezone).is_err(), "{timezone}");
        }
    }

    #[test]
    fn java_short_ids_match_their_regions_in_winter_and_summer() {
        let aliases = [
            ("ACT", "Australia/Darwin"),
            ("AET", "Australia/Sydney"),
            ("AGT", "America/Argentina/Buenos_Aires"),
            ("ART", "Africa/Cairo"),
            ("AST", "America/Anchorage"),
            ("BET", "America/Sao_Paulo"),
            ("BST", "Asia/Dhaka"),
            ("CAT", "Africa/Harare"),
            ("CNT", "America/St_Johns"),
            ("CST", "America/Chicago"),
            ("CTT", "Asia/Shanghai"),
            ("EAT", "Africa/Addis_Ababa"),
            ("ECT", "Europe/Paris"),
            ("IET", "America/Indiana/Indianapolis"),
            ("IST", "Asia/Kolkata"),
            ("JST", "Asia/Tokyo"),
            ("MIT", "Pacific/Apia"),
            ("NET", "Asia/Yerevan"),
            ("NST", "Pacific/Auckland"),
            ("PLT", "Asia/Karachi"),
            ("PNT", "America/Phoenix"),
            ("PRT", "America/Puerto_Rico"),
            ("PST", "America/Los_Angeles"),
            ("SST", "Pacific/Guadalcanal"),
            ("VST", "Asia/Ho_Chi_Minh"),
            ("EST", "-05:00"),
            ("HST", "-10:00"),
            ("MST", "-07:00"),
        ];
        for (alias, canonical) in aliases {
            for timestamp in [1_704_067_200_123_456, 1_719_792_000_123_456] {
                assert_eq!(
                    shift(alias, timestamp),
                    shift(canonical, timestamp),
                    "{alias}"
                );
            }
        }
    }

    #[test]
    fn system_v_zones_match_java_transition_boundaries() {
        let zones = [
            ("SystemV/AST4ADT", -4),
            ("SystemV/EST5EDT", -5),
            ("SystemV/CST6CDT", -6),
            ("SystemV/MST7MDT", -7),
            ("SystemV/PST8PDT", -8),
            ("SystemV/YST9YDT", -9),
        ];
        for (timezone, hours) in zones {
            let standard = hours * 3600;
            for (local, offset_before, offset_after) in [
                ("1900-04-29T02:00:00Z", standard, standard + 3600),
                ("1974-01-06T02:00:00Z", standard, standard + 3600),
                ("1974-11-24T02:00:00Z", standard + 3600, standard),
                ("1975-02-23T02:00:00Z", standard, standard + 3600),
                ("2024-04-28T02:00:00Z", standard, standard + 3600),
                ("2024-10-27T02:00:00Z", standard + 3600, standard),
            ] {
                let transition = DateTime::parse_from_rfc3339(local)
                    .unwrap()
                    .timestamp_micros()
                    - i64::from(offset_before) * 1_000_000;
                assert_eq!(
                    shift(timezone, transition - 1),
                    transition - 1 + i64::from(offset_before) * 1_000_000,
                    "{timezone} before {local}"
                );
                assert_eq!(
                    shift(timezone, transition),
                    transition + i64::from(offset_after) * 1_000_000,
                    "{timezone} at {local}"
                );
            }
            let before_transitions = DateTime::parse_from_rfc3339("1899-07-01T00:00:00Z")
                .unwrap()
                .timestamp_micros();
            assert_eq!(
                shift(timezone, before_transitions),
                before_transitions + i64::from(standard) * 1_000_000
            );
        }
        for (timezone, hours) in [
            ("SystemV/AST4", -4),
            ("SystemV/EST5", -5),
            ("SystemV/CST6", -6),
            ("SystemV/MST7", -7),
            ("SystemV/PST8", -8),
            ("SystemV/YST9", -9),
            ("SystemV/HST10", -10),
        ] {
            assert_eq!(
                shift(timezone, 1_719_792_000_000_000),
                1_719_792_000_000_000 + i64::from(hours) * 3_600_000_000
            );
        }
    }
}
