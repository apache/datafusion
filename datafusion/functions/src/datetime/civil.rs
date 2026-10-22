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

//! Proleptic Gregorian calendar arithmetic on plain integers.
//!
//! `date_trunc` and `date_bin` both need to move a timestamp to a calendar
//! boundary. Doing that through `chrono` means building a validated
//! `NaiveDateTime` per value, and for `date_bin` it also caps the input range to
//! what `DateTime<Utc>` can represent, which is narrower than a
//! `Timestamp(Second)` column (#25855). These helpers work directly on day
//! counts and civil fields instead.

use arrow::temporal_conversions::NANOSECONDS_IN_DAY;

// The two helpers below duplicate `chrono::NaiveDate::{from_epoch_days,
// to_epoch_days}`. They are kept separate because chrono's versions round trip
// through a validated `NaiveDate`: `from_epoch_days` computes year flags and
// returns an `Option`, and reading the year/month/day back out decodes them from
// its packed representation. These helpers stay in plain integers, which is all
// their callers need.

/// Days from the Unix epoch to 0000-03-01, the epoch used by the civil calendar
/// conversions in this module.
const DAYS_EPOCH_SHIFT: i64 = 719_468;

/// Days in a 400 year era of the proleptic Gregorian calendar.
const DAYS_PER_ERA: i64 = 146_097;

/// Splits a day count relative to the Unix epoch into a proleptic Gregorian
/// year, month (1-12) and day of month (1-31).
///
/// This is a port of Howard Hinnant's `civil_from_days`, which documents the
/// derivation of the constants and the March-based year used below:
/// <https://howardhinnant.github.io/date_algorithms.html#civil_from_days>
pub(crate) fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + DAYS_EPOCH_SHIFT;
    let era = z.div_euclid(DAYS_PER_ERA);
    let day_of_era = z.rem_euclid(DAYS_PER_ERA);
    let year_of_era = (day_of_era - day_of_era / 1460 + day_of_era / 36524
        - day_of_era / 146_096)
        / 365;
    let day_of_year =
        day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    // Month index with March as 0, so that the leap day falls at the end of the year.
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// Inverse of [`civil_from_days`]: the day count relative to the Unix epoch for
/// the given proleptic Gregorian date.
///
/// This is a port of Howard Hinnant's `days_from_civil`, which documents the
/// derivation of the constants and the March-based year used below:
/// <https://howardhinnant.github.io/date_algorithms.html#days_from_civil>
pub(crate) fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year.rem_euclid(400);
    let month_index = if month > 2 { month - 3 } else { month + 9 };
    let day_of_year = (153 * month_index + 2) / 5 + day - 1;
    let day_of_era =
        year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * DAYS_PER_ERA + day_of_era - DAYS_EPOCH_SHIFT
}

/// A timestamp split into proleptic Gregorian calendar fields.
///
/// Ordering is derived, and the field order above is chronological, so the
/// derive is correct as long as both invariants hold:
///
/// * `day` is a valid day of `month`, and
/// * `tod_nanos` is normalized to `[0, NANOSECONDS_IN_DAY)`.
///
/// Every constructor in this module maintains both, so do not reorder the
/// fields and do not build one by hand outside it.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(crate) struct CivilTimestamp {
    pub(crate) year: i64,
    pub(crate) month: i64,
    pub(crate) day: i64,
    pub(crate) tod_nanos: i64,
}

impl CivilTimestamp {
    fn from_days_and_tod(days: i64, tod_nanos: i64) -> Self {
        let (year, month, day) = civil_from_days(days);
        Self {
            year,
            month,
            day,
            tod_nanos,
        }
    }

    /// Splits `nanos` since the Unix epoch into calendar fields.
    pub(crate) fn from_nanos(nanos: i64) -> Self {
        Self::from_days_and_tod(
            nanos.div_euclid(NANOSECONDS_IN_DAY),
            nanos.rem_euclid(NANOSECONDS_IN_DAY),
        )
    }

    /// [`CivilTimestamp::from_nanos`] for timestamps too coarse to fit `i64`
    /// nanoseconds. `None` when the day count itself overflows `i64`.
    pub(crate) fn from_nanos_wide(nanos: i128) -> Option<Self> {
        let nanos_per_day = i128::from(NANOSECONDS_IN_DAY);
        let days = i64::try_from(nanos.div_euclid(nanos_per_day)).ok()?;
        Some(Self::from_days_and_tod(
            days,
            nanos.rem_euclid(nanos_per_day) as i64,
        ))
    }

    /// Nanoseconds since the Unix epoch, or `None` if that overflows `i64`.
    pub(crate) fn to_nanos(self) -> Option<i64> {
        days_from_civil(self.year, self.month, self.day)
            .checked_mul(NANOSECONDS_IN_DAY)?
            .checked_add(self.tod_nanos)
    }

    /// [`CivilTimestamp::to_nanos`] in `i128`, which cannot overflow for any
    /// value this module can construct.
    pub(crate) fn to_nanos_wide(self) -> i128 {
        i128::from(days_from_civil(self.year, self.month, self.day))
            * i128::from(NANOSECONDS_IN_DAY)
            + i128::from(self.tod_nanos)
    }

    /// Months since year 0, so that two timestamps can be compared by month
    /// without regard to day or time of day.
    pub(crate) fn month_index(self) -> i64 {
        self.year * 12 + self.month
    }

    /// Shifts by whole months, keeping the day and time of day. A day that the
    /// target month does not have clamps to that month's last day, matching
    /// `chrono`'s `checked_add_months`.
    pub(crate) fn shift_months(self, month_delta: i64) -> Self {
        let total = self.month - 1 + month_delta;
        let year = self.year + total.div_euclid(12);
        let month = total.rem_euclid(12) + 1;
        Self {
            year,
            month,
            day: self.day.min(days_in_month(year, month)),
            tod_nanos: self.tod_nanos,
        }
    }
}

/// Length of `month` (1-12) in `year`, in days.
fn days_in_month(year: i64, month: i64) -> i64 {
    const LENGTHS: [i64; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if month == 2 && year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
        29
    } else {
        LENGTHS[(month - 1) as usize]
    }
}
