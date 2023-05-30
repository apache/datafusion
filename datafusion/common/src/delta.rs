// MIT License
//
// Copyright (c) 2020-2022 Oliver Margetts
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// Copied from chronoutil crate

//! Contains utility functions for shifting Date objects.
use chrono::Datelike;

/// Returns true if the year is a leap-year, as naively defined in the Gregorian calendar.
#[inline]
fn is_leap_year(year: i32) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

// If the day lies within the month, this function has no effect. Otherwise, it shifts
// day backwards to the final day of the month.
// XXX: No attempt is made to handle days outside the 1-31 range.
#[inline]
fn normalise_day(year: i32, month: u32, day: u32) -> u32 {
    if day <= 28 {
        day
    } else if month == 2 {
        28 + is_leap_year(year) as u32
    } else if day == 31 && (month == 4 || month == 6 || month == 9 || month == 11) {
        30
    } else {
        day
    }
}

/// Shift a date by the given number of months.
/// Ambiguous month-ends are shifted backwards as necessary.
pub fn shift_months<D: Datelike>(date: D, months: i32, sign: i32) -> D {
    let months = months * sign;
    let mut year = date.year() + (date.month() as i32 + months) / 12;
    let mut month = (date.month() as i32 + months) % 12;
    let mut day = date.day();

    if month < 1 {
        year -= 1;
        month += 12;
    }

    day = normalise_day(year, month as u32, day);

    // This is slow but guaranteed to succeed (short of interger overflow)
    if day <= 28 {
        date.with_day(day)
            .unwrap()
            .with_month(month as u32)
            .unwrap()
            .with_year(year)
            .unwrap()
    } else {
        date.with_day(1)
            .unwrap()
            .with_month(month as u32)
            .unwrap()
            .with_year(year)
            .unwrap()
            .with_day(day)
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use chrono::naive::{NaiveDate, NaiveDateTime, NaiveTime};

    use super::*;

    #[test]
    fn test_leap_year_cases() {
        let _leap_years: Vec<i32> = vec![
            1904, 1908, 1912, 1916, 1920, 1924, 1928, 1932, 1936, 1940, 1944, 1948, 1952,
            1956, 1960, 1964, 1968, 1972, 1976, 1980, 1984, 1988, 1992, 1996, 2000, 2004,
            2008, 2012, 2016, 2020,
        ];
        let leap_years_1900_to_2020: HashSet<i32> = _leap_years.into_iter().collect();

        for year in 1900..2021 {
            assert_eq!(is_leap_year(year), leap_years_1900_to_2020.contains(&year))
        }
    }

    #[test]
    fn test_shift_months() {
        let base = NaiveDate::from_ymd_opt(2020, 1, 31).unwrap();

        assert_eq!(
            shift_months(base, 0, 1),
            NaiveDate::from_ymd_opt(2020, 1, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, 1, 1),
            NaiveDate::from_ymd_opt(2020, 2, 29).unwrap()
        );
        assert_eq!(
            shift_months(base, 2, 1),
            NaiveDate::from_ymd_opt(2020, 3, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, 3, 1),
            NaiveDate::from_ymd_opt(2020, 4, 30).unwrap()
        );
        assert_eq!(
            shift_months(base, 4, 1),
            NaiveDate::from_ymd_opt(2020, 5, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, 5, 1),
            NaiveDate::from_ymd_opt(2020, 6, 30).unwrap()
        );
        assert_eq!(
            shift_months(base, 6, 1),
            NaiveDate::from_ymd_opt(2020, 7, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, 7, 1),
            NaiveDate::from_ymd_opt(2020, 8, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, 8, 1),
            NaiveDate::from_ymd_opt(2020, 9, 30).unwrap()
        );
        assert_eq!(
            shift_months(base, 9, 1),
            NaiveDate::from_ymd_opt(2020, 10, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, 10, 1),
            NaiveDate::from_ymd_opt(2020, 11, 30).unwrap()
        );
        assert_eq!(
            shift_months(base, 11, 1),
            NaiveDate::from_ymd_opt(2020, 12, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, 12, 1),
            NaiveDate::from_ymd_opt(2021, 1, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, 13, 1),
            NaiveDate::from_ymd_opt(2021, 2, 28).unwrap()
        );

        assert_eq!(
            shift_months(base, 1, -1),
            NaiveDate::from_ymd_opt(2019, 12, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, 2, -1),
            NaiveDate::from_ymd_opt(2019, 11, 30).unwrap()
        );
        assert_eq!(
            shift_months(base, 3, -1),
            NaiveDate::from_ymd_opt(2019, 10, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, 4, -1),
            NaiveDate::from_ymd_opt(2019, 9, 30).unwrap()
        );
        assert_eq!(
            shift_months(base, 5, -1),
            NaiveDate::from_ymd_opt(2019, 8, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, 6, -1),
            NaiveDate::from_ymd_opt(2019, 7, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, 7, -1),
            NaiveDate::from_ymd_opt(2019, 6, 30).unwrap()
        );
        assert_eq!(
            shift_months(base, 8, -1),
            NaiveDate::from_ymd_opt(2019, 5, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, 9, -1),
            NaiveDate::from_ymd_opt(2019, 4, 30).unwrap()
        );
        assert_eq!(
            shift_months(base, 10, -1),
            NaiveDate::from_ymd_opt(2019, 3, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, 11, -1),
            NaiveDate::from_ymd_opt(2019, 2, 28).unwrap()
        );
        assert_eq!(
            shift_months(base, 12, -1),
            NaiveDate::from_ymd_opt(2019, 1, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, 13, -1),
            NaiveDate::from_ymd_opt(2018, 12, 31).unwrap()
        );

        assert_eq!(
            shift_months(base, 1265, 1),
            NaiveDate::from_ymd_opt(2125, 6, 30).unwrap()
        );
    }

    #[test]
    fn test_shift_months_with_overflow() {
        let base = NaiveDate::from_ymd_opt(2020, 12, 31).unwrap();

        assert_eq!(shift_months(base, 0, 1), base);
        assert_eq!(
            shift_months(base, 1, 1),
            NaiveDate::from_ymd_opt(2021, 1, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, 2, 1),
            NaiveDate::from_ymd_opt(2021, 2, 28).unwrap()
        );
        assert_eq!(
            shift_months(base, 12, 1),
            NaiveDate::from_ymd_opt(2021, 12, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, 18, 1),
            NaiveDate::from_ymd_opt(2022, 6, 30).unwrap()
        );

        assert_eq!(
            shift_months(base, 1, -1),
            NaiveDate::from_ymd_opt(2020, 11, 30).unwrap()
        );
        assert_eq!(
            shift_months(base, 2, -1),
            NaiveDate::from_ymd_opt(2020, 10, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, 10, -1),
            NaiveDate::from_ymd_opt(2020, 2, 29).unwrap()
        );
        assert_eq!(
            shift_months(base, 12, -1),
            NaiveDate::from_ymd_opt(2019, 12, 31).unwrap()
        );
        assert_eq!(
            shift_months(base, 18, -1),
            NaiveDate::from_ymd_opt(2019, 6, 30).unwrap()
        );
    }

    #[test]
    fn test_shift_months_datetime() {
        let date = NaiveDate::from_ymd_opt(2020, 1, 31).unwrap();
        let o_clock = NaiveTime::from_hms_opt(1, 2, 3).unwrap();

        let base = NaiveDateTime::new(date, o_clock);

        assert_eq!(
            shift_months(base, 0, 1).date(),
            NaiveDate::from_ymd_opt(2020, 1, 31).unwrap(),
        );
        assert_eq!(
            shift_months(base, 1, 1).date(),
            NaiveDate::from_ymd_opt(2020, 2, 29).unwrap(),
        );
        assert_eq!(
            shift_months(base, 2, 1).date(),
            NaiveDate::from_ymd_opt(2020, 3, 31).unwrap(),
        );
        assert_eq!(shift_months(base, 0, 1).time(), o_clock);
        assert_eq!(shift_months(base, 1, 1).time(), o_clock);
        assert_eq!(shift_months(base, 2, 1).time(), o_clock);
    }

    #[test]
    fn add_11_months() {
        let prior = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let actual = shift_months(prior, 11, 1);
        assert_eq!(format!("{actual:?}").as_str(), "2000-12-01");
    }

    #[test]
    fn add_12_months() {
        let prior = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let actual = shift_months(prior, 12, 1);
        assert_eq!(format!("{actual:?}").as_str(), "2001-01-01");
    }

    #[test]
    fn add_13_months() {
        let prior = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let actual = shift_months(prior, 13, 1);
        assert_eq!(format!("{actual:?}").as_str(), "2001-02-01");
    }

    #[test]
    fn sub_11_months() {
        let prior = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let actual = shift_months(prior, 11, -1);
        assert_eq!(format!("{actual:?}").as_str(), "1999-02-01");
    }

    #[test]
    fn sub_12_months() {
        let prior = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let actual = shift_months(prior, 12, -1);
        assert_eq!(format!("{actual:?}").as_str(), "1999-01-01");
    }

    #[test]
    fn sub_13_months() {
        let prior = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let actual = shift_months(prior, 13, -1);
        assert_eq!(format!("{actual:?}").as_str(), "1998-12-01");
    }
}
