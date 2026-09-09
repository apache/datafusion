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

//! Casting a timezone-naive `Timestamp(_, None)` to a `Timestamp(_, Some(tz))`.
//!
//! Casting a naive (local) timestamp into a named timezone means interpreting a
//! wall clock reading in that timezone. Around a daylight saving transition two
//! wall clock readings are not a single instant:
//!
//! * **Ambiguous** — the hour repeated by a "fall back" transition. For example
//!   `2024-11-03T01:30:00` occurs twice in `America/New_York`, once at
//!   `-04:00` (EDT) and once at `-05:00` (EST).
//! * **Nonexistent** — the hour skipped by a "spring forward" transition. For
//!   example `2024-03-10T02:30:00` never happens in `America/New_York`.
//!
//! [`arrow::compute::cast`] resolves the offset with
//! `offset_from_local_datetime(..).single()`, which is `None` for both cases, so
//! these casts fail with `Cannot cast timezone to different timezone` (or become
//! NULL when `CastOptions::safe` is set). PostgreSQL and DuckDB instead resolve
//! both deterministically, and this module implements their convention:
//!
//! * ambiguous — pick the **later** instant, i.e. the post-transition (standard)
//!   offset, so `2024-11-03T01:30:00` in `America/New_York` is
//!   `2024-11-03T01:30:00-05:00`;
//! * nonexistent — shift **forward** by the size of the gap, which is the same
//!   as interpreting the wall clock reading with the pre-transition offset, so
//!   `2024-03-10T02:30:00` in `America/New_York` is `2024-03-10T03:30:00-04:00`.
//!
//! Because this is the only pair of types where DataFusion's semantics differ
//! from the arrow kernel, DataFusion's cast entry points special-case exactly
//! that pair (see [`is_naive_to_timezone_cast`]) and delegate everything else,
//! including the unit conversion performed here, to arrow.

use std::sync::Arc;

use arrow::array::timezone::Tz;
use arrow::array::{Array, ArrayRef, AsArray, PrimitiveArray};
use arrow::compute::CastOptions;
use arrow::compute::kernels::cast::cast_with_options;
use arrow::datatypes::{
    ArrowTimestampType, DataType, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use arrow::error::ArrowError;
use arrow::temporal_conversions::as_datetime;
use chrono::{Duration, FixedOffset, NaiveDateTime, Offset, TimeZone};

use crate::Result;
use crate::error::_internal_err;

/// Returns `true` if this is a cast from a timezone-naive timestamp to a
/// timestamp with a timezone, the one cast handled by
/// [`cast_naive_timestamp_to_timezone`].
pub fn is_naive_to_timezone_cast(from: &DataType, to: &DataType) -> bool {
    matches!(
        (from, to),
        (
            DataType::Timestamp(_, None),
            DataType::Timestamp(_, Some(_))
        )
    )
}

/// Casts a timezone-naive timestamp array to `to_type`, a timestamp with a
/// timezone, resolving daylight saving ambiguities and gaps the way PostgreSQL
/// and DuckDB do (see the [module docs](self)).
///
/// `to_type` must be a `Timestamp(_, Some(tz))` and `array` a
/// `Timestamp(_, None)`; use [`is_naive_to_timezone_cast`] to check.
pub fn cast_naive_timestamp_to_timezone(
    array: &ArrayRef,
    to_type: &DataType,
    cast_options: &CastOptions<'static>,
) -> Result<ArrayRef> {
    let (
        DataType::Timestamp(from_unit, None),
        DataType::Timestamp(to_unit, Some(tz_str)),
    ) = (array.data_type(), to_type)
    else {
        return _internal_err!(
            "cast_naive_timestamp_to_timezone expects a naive timestamp source and a \
             timezone-aware timestamp target, got {} and {to_type}",
            array.data_type()
        );
    };

    // Let arrow do the unit conversion (and its overflow handling) first, so
    // that only the timezone adjustment is left to do here.
    let array = if from_unit == to_unit {
        Arc::clone(array)
    } else {
        cast_with_options(array, &DataType::Timestamp(*to_unit, None), cast_options)?
    };

    let tz: Tz = tz_str.parse()?;

    let adjusted: ArrayRef = match to_unit {
        TimeUnit::Second => {
            adjust::<TimestampSecondType>(&array, &tz, tz_str, cast_options)?
        }
        TimeUnit::Millisecond => {
            adjust::<TimestampMillisecondType>(&array, &tz, tz_str, cast_options)?
        }
        TimeUnit::Microsecond => {
            adjust::<TimestampMicrosecondType>(&array, &tz, tz_str, cast_options)?
        }
        TimeUnit::Nanosecond => {
            adjust::<TimestampNanosecondType>(&array, &tz, tz_str, cast_options)?
        }
    };

    Ok(adjusted)
}

/// Reinterprets every naive value of `array` as a wall clock reading in `tz`.
fn adjust<T: ArrowTimestampType>(
    array: &ArrayRef,
    tz: &Tz,
    tz_str: &Arc<str>,
    cast_options: &CastOptions<'static>,
) -> Result<ArrayRef> {
    let array: &PrimitiveArray<T> = array.as_primitive::<T>();

    let adjust = |value: i64| -> Option<i64> {
        let local = as_datetime::<T>(value)?;
        let offset = resolve_local_offset(tz, local)?;
        T::from_naive_datetime(local - offset, None)
    };

    let adjusted: PrimitiveArray<T> = if cast_options.safe {
        array.unary_opt::<_, T>(adjust)
    } else {
        array.try_unary::<_, T, _>(|value| {
            adjust(value).ok_or_else(|| {
                ArrowError::CastError(
                    "Cannot cast timezone to different timezone".to_string(),
                )
            })
        })?
    };

    Ok(Arc::new(adjusted.with_timezone(Arc::clone(tz_str))) as ArrayRef)
}

/// Resolves the UTC offset that a wall clock reading `local` has in `tz`,
/// following the PostgreSQL/DuckDB convention described in the
/// [module docs](self).
///
/// Returns `None` only if the offset cannot be determined at all, which keeps
/// the error (or NULL, under `CastOptions::safe`) that arrow would produce.
fn resolve_local_offset(tz: &Tz, local: NaiveDateTime) -> Option<FixedOffset> {
    match tz.offset_from_local_datetime(&local) {
        chrono::LocalResult::Single(offset) => Some(offset.fix()),
        // The wall clock reading happens twice, once before and once after a
        // "fall back" transition. chrono returns the offsets in chronological
        // order of the two instants, so the second one is the later instant.
        chrono::LocalResult::Ambiguous(_earlier, later) => Some(later.fix()),
        // The wall clock reading is inside a "spring forward" gap. Shifting it
        // forward by the size of the gap is the same as interpreting it with
        // the offset in effect before the transition, which we recover by
        // probing a day earlier: no tzdb entry has two transitions within 24
        // hours of each other, so a day before a gap is always outside it.
        chrono::LocalResult::None => tz
            .offset_from_local_datetime(&(local - Duration::hours(24)))
            .earliest()
            .map(|offset| offset.fix()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{TimestampMillisecondArray, TimestampNanosecondArray};
    use arrow::datatypes::TimeUnit;
    use chrono::NaiveDate;

    const NEW_YORK: &str = "America/New_York";
    const SYDNEY: &str = "Australia/Sydney";

    fn local(s: &str) -> NaiveDateTime {
        s.parse().unwrap()
    }

    fn nanos(values: Vec<Option<i64>>) -> ArrayRef {
        Arc::new(TimestampNanosecondArray::from(values)) as ArrayRef
    }

    /// Casts `values` (nanosecond naive timestamps) to `Timestamp(Nanosecond, Some(tz))`
    /// and returns the resulting instants as epoch seconds.
    fn cast_to_tz(values: Vec<Option<i64>>, tz: &str) -> Vec<Option<i64>> {
        let array = nanos(values);
        let to_type = DataType::Timestamp(TimeUnit::Nanosecond, Some(tz.into()));
        let out =
            cast_naive_timestamp_to_timezone(&array, &to_type, &CastOptions::default())
                .unwrap();
        assert_eq!(out.data_type(), &to_type);
        out.as_primitive::<TimestampNanosecondType>()
            .iter()
            .map(|v| v.map(|v| v / 1_000_000_000))
            .collect()
    }

    fn naive_nanos(s: &str) -> i64 {
        local(s).and_utc().timestamp_nanos_opt().unwrap()
    }

    #[test]
    fn is_naive_to_timezone_cast_only_matches_that_pair() {
        let naive = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let aware = DataType::Timestamp(TimeUnit::Second, Some("UTC".into()));
        assert!(is_naive_to_timezone_cast(&naive, &aware));
        assert!(!is_naive_to_timezone_cast(&aware, &naive));
        assert!(!is_naive_to_timezone_cast(&aware, &aware));
        assert!(!is_naive_to_timezone_cast(&naive, &naive));
        assert!(!is_naive_to_timezone_cast(&DataType::Date32, &aware));
    }

    #[test]
    fn resolves_unambiguous_local_times() {
        let tz: Tz = NEW_YORK.parse().unwrap();
        // Eastern Daylight Time
        assert_eq!(
            resolve_local_offset(&tz, local("2024-11-01T00:00:00")).unwrap(),
            FixedOffset::east_opt(-4 * 3600).unwrap()
        );
        // Eastern Standard Time
        assert_eq!(
            resolve_local_offset(&tz, local("2024-12-01T00:00:00")).unwrap(),
            FixedOffset::east_opt(-5 * 3600).unwrap()
        );
    }

    #[test]
    fn ambiguous_local_time_picks_the_later_instant() {
        let ny: Tz = NEW_YORK.parse().unwrap();
        // 2024-11-03T01:30 happens twice: at -04:00 and then at -05:00.
        assert_eq!(
            resolve_local_offset(&ny, local("2024-11-03T01:30:00")).unwrap(),
            FixedOffset::east_opt(-5 * 3600).unwrap()
        );

        let sydney: Tz = SYDNEY.parse().unwrap();
        // 2024-04-07T02:30 happens twice: at +11:00 and then at +10:00.
        assert_eq!(
            resolve_local_offset(&sydney, local("2024-04-07T02:30:00")).unwrap(),
            FixedOffset::east_opt(10 * 3600).unwrap()
        );
    }

    #[test]
    fn nonexistent_local_time_shifts_forward_by_the_gap() {
        let ny: Tz = NEW_YORK.parse().unwrap();
        // 2024-03-10T02:30 does not exist; the pre-transition offset is -05:00.
        assert_eq!(
            resolve_local_offset(&ny, local("2024-03-10T02:30:00")).unwrap(),
            FixedOffset::east_opt(-5 * 3600).unwrap()
        );

        let sydney: Tz = SYDNEY.parse().unwrap();
        // 2024-10-06T02:30 does not exist; the pre-transition offset is +10:00.
        assert_eq!(
            resolve_local_offset(&sydney, local("2024-10-06T02:30:00")).unwrap(),
            FixedOffset::east_opt(10 * 3600).unwrap()
        );
    }

    #[test]
    fn fixed_offset_timezones_are_never_ambiguous() {
        let tz: Tz = "+08:00".parse().unwrap();
        for s in [
            "2024-11-03T01:30:00",
            "2024-03-10T02:30:00",
            "2024-01-01T00:00:00",
        ] {
            assert_eq!(
                resolve_local_offset(&tz, local(s)).unwrap(),
                FixedOffset::east_opt(8 * 3600).unwrap()
            );
        }
    }

    #[test]
    fn cast_resolves_dst_boundaries() {
        // Unambiguous: 2024-11-01T00:00 EDT (-04:00) == 2024-11-01T04:00Z.
        let expected = NaiveDate::from_ymd_opt(2024, 11, 1)
            .unwrap()
            .and_hms_opt(4, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp();
        assert_eq!(
            cast_to_tz(vec![Some(naive_nanos("2024-11-01T00:00:00"))], NEW_YORK),
            vec![Some(expected)]
        );

        // Ambiguous: the later instant, 01:30-05:00 == 06:30Z == 1730615400.
        // (The earlier candidate, 01:30-04:00, would be 1730611800.)
        assert_eq!(
            cast_to_tz(vec![Some(naive_nanos("2024-11-03T01:30:00"))], NEW_YORK),
            vec![Some(1730615400)]
        );

        // Gap: shifted forward to 03:30-04:00 == 07:30Z == 1710055800.
        assert_eq!(
            cast_to_tz(vec![Some(naive_nanos("2024-03-10T02:30:00"))], NEW_YORK),
            vec![Some(1710055800)]
        );

        // Sydney: ambiguous 02:30 resolves to +10:00 == 2024-04-06T16:30Z.
        let expected = NaiveDate::from_ymd_opt(2024, 4, 6)
            .unwrap()
            .and_hms_opt(16, 30, 0)
            .unwrap()
            .and_utc()
            .timestamp();
        assert_eq!(
            cast_to_tz(vec![Some(naive_nanos("2024-04-07T02:30:00"))], SYDNEY),
            vec![Some(expected)]
        );

        // Sydney: gap 02:30 shifts to 03:30+11:00 == 2024-10-05T16:30Z.
        let expected = NaiveDate::from_ymd_opt(2024, 10, 5)
            .unwrap()
            .and_hms_opt(16, 30, 0)
            .unwrap()
            .and_utc()
            .timestamp();
        assert_eq!(
            cast_to_tz(vec![Some(naive_nanos("2024-10-06T02:30:00"))], SYDNEY),
            vec![Some(expected)]
        );
    }

    #[test]
    fn cast_preserves_nulls() {
        assert_eq!(
            cast_to_tz(
                vec![None, Some(naive_nanos("2024-11-03T01:30:00")), None],
                NEW_YORK
            ),
            vec![None, Some(1730615400), None]
        );
    }

    #[test]
    fn cast_matches_arrow_when_the_local_time_is_unambiguous() {
        let options = CastOptions::default();
        for (values, tz) in [
            (vec![Some(naive_nanos("2024-11-03T01:30:00"))], "+08:00"),
            (
                vec![Some(naive_nanos("2024-06-15T12:00:00")), None],
                NEW_YORK,
            ),
        ] {
            let array = nanos(values);
            let to_type = DataType::Timestamp(TimeUnit::Nanosecond, Some(tz.into()));
            let ours =
                cast_naive_timestamp_to_timezone(&array, &to_type, &options).unwrap();
            let arrows = cast_with_options(&array, &to_type, &options).unwrap();
            assert_eq!(&ours, &arrows, "timezone {tz}");
        }
    }

    #[test]
    fn cast_converts_the_time_unit_too() {
        let array = nanos(vec![Some(naive_nanos("2024-11-03T01:30:00")), None]);
        let to_type = DataType::Timestamp(TimeUnit::Millisecond, Some(NEW_YORK.into()));
        let out =
            cast_naive_timestamp_to_timezone(&array, &to_type, &CastOptions::default())
                .unwrap();
        assert_eq!(out.data_type(), &to_type);
        let out = out
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(out.value(0), 1730615400 * 1_000);
        assert!(out.is_null(1));
    }
}
