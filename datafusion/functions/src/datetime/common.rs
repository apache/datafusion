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

use std::fmt;
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::timezone::Tz;
use arrow::array::{
    Array, ArrowPrimitiveType, AsArray, GenericStringArray, PrimitiveArray,
    StringArrayType, StringViewArray,
};
use arrow::compute::kernels::cast_utils::string_to_timestamp_nanos;
use arrow::datatypes::DataType;
use chrono::format::{parse, Parsed, StrftimeItems};
use chrono::LocalResult::Single;
use chrono::{DateTime, FixedOffset, LocalResult, NaiveDateTime, TimeZone, Utc};

use datafusion_common::cast::as_generic_string_array;
use datafusion_common::config::ConfigOptions;
use datafusion_common::{
    exec_datafusion_err, exec_err, unwrap_or_internal_err, DataFusionError, Result,
    ScalarType, ScalarValue,
};
use datafusion_expr::ColumnarValue;

/// Error message if nanosecond conversion request beyond supported interval
const ERR_NANOSECONDS_NOT_SUPPORTED: &str = "The dates that can be represented as nanoseconds have to be between 1677-09-21T00:12:44.0 and 2262-04-11T23:47:16.854775804";

/// Calls string_to_timestamp_nanos and converts the error type
pub(crate) fn string_to_timestamp_nanos_shim(s: &str) -> Result<i64> {
    string_to_timestamp_nanos(s).map_err(|e| e.into())
}

#[derive(Clone, Copy, Debug)]
enum ConfiguredZone {
    Named(Tz),
    Offset(FixedOffset),
}

#[derive(Clone)]
pub(crate) struct ConfiguredTimeZone {
    repr: Arc<str>,
    zone: ConfiguredZone,
}

impl ConfiguredTimeZone {
    pub(crate) fn utc() -> Self {
        Self {
            repr: Arc::from("+00:00"),
            zone: ConfiguredZone::Offset(FixedOffset::east_opt(0).unwrap()),
        }
    }

    pub(crate) fn parse(tz: &str) -> Result<Option<Self>> {
        let tz = tz.trim();
        if tz.is_empty() {
            return Ok(None);
        }

        if let Ok(named) = Tz::from_str(tz) {
            return Ok(Some(Self {
                repr: Arc::from(tz),
                zone: ConfiguredZone::Named(named),
            }));
        }

        if let Some(offset) = parse_fixed_offset(tz) {
            return Ok(Some(Self {
                repr: Arc::from(tz),
                zone: ConfiguredZone::Offset(offset),
            }));
        }

        Err(exec_datafusion_err!(
            "Invalid execution timezone '{tz}'. Please provide an IANA timezone name (e.g. 'America/New_York') or an offset in the form '+HH:MM'."
        ))
    }

    pub(crate) fn from_config(config: &ConfigOptions) -> Self {
        match Self::parse(config.execution.time_zone.as_deref().unwrap_or("")) {
            Ok(Some(tz)) => tz,
            _ => Self::utc(),
        }
    }

    fn timestamp_from_naive(&self, naive: &NaiveDateTime) -> Result<i64> {
        match self.zone {
            ConfiguredZone::Named(tz) => {
                local_datetime_to_timestamp(tz.from_local_datetime(naive), &self.repr)
            }
            ConfiguredZone::Offset(offset) => {
                local_datetime_to_timestamp(offset.from_local_datetime(naive), &self.repr)
            }
        }
    }

    fn datetime_from_formatted(&self, s: &str, format: &str) -> Result<DateTime<Utc>> {
        let datetime = match self.zone {
            ConfiguredZone::Named(tz) => {
                string_to_datetime_formatted(&tz, s, format)?.with_timezone(&Utc)
            }
            ConfiguredZone::Offset(offset) => {
                string_to_datetime_formatted(&offset, s, format)?.with_timezone(&Utc)
            }
        };
        Ok(datetime)
    }
}

impl fmt::Debug for ConfiguredTimeZone {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConfiguredTimeZone")
            .field("repr", &self.repr)
            .finish()
    }
}

impl PartialEq for ConfiguredTimeZone {
    fn eq(&self, other: &Self) -> bool {
        self.repr == other.repr
    }
}

impl Eq for ConfiguredTimeZone {}

impl Hash for ConfiguredTimeZone {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.repr.hash(state);
    }
}

fn parse_fixed_offset(tz: &str) -> Option<FixedOffset> {
    let tz = tz.trim();
    if tz.eq_ignore_ascii_case("utc") || tz.eq_ignore_ascii_case("z") {
        return FixedOffset::east_opt(0);
    }

    // Fast/robust path: try to parse the offset using chrono's `Parsed` API.
    // Chrono accepts offsets in a few formats (%z = +HHMM, %:z = +HH:MM, %::z = +HH:MM:SS).
    // We normalize compact digit-only offsets (e.g. +05, +0500, +053045) into
    // a colon-separated form so chrono can parse them with the %:z / %::z
    // patterns.
    if let Some(first) = tz.chars().next() {
        if first == '+' || first == '-' {
            let sign = first;
            let rest = &tz[1..];

            // If rest is digits-only, normalize based on length:
            // 2  -> HH      -> HH:00
            // 4  -> HHMM    -> HH:MM
            // 6  -> HHMMSS  -> HH:MM:SS
            let normalized = if rest.chars().all(|c| c.is_ascii_digit()) {
                match rest.len() {
                    2 => format!("{}{}:00", sign, &rest[0..2]),
                    4 => format!("{}{}:{}", sign, &rest[0..2], &rest[2..4]),
                    6 => {
                        format!("{}{}:{}:{}", sign, &rest[0..2], &rest[2..4], &rest[4..6])
                    }
                    _ => tz.to_string(),
                }
            } else {
                // otherwise keep original (may already contain colons)
                tz.to_string()
            };

            // Try %::z (HH:MM:SS), then %:z (HH:MM), then %z (HHMM)
            let try_formats = ["%::z", "%:z", "%z"];
            for fmt in try_formats {
                let mut parsed = Parsed::new();
                if parse(&mut parsed, &normalized, StrftimeItems::new(fmt)).is_ok() {
                    if let Ok(off) = parsed.to_fixed_offset() {
                        return Some(off);
                    }
                }
            }
        }
    }

    // Fallback: attempt a conservative manual parse to preserve previous behavior
    // for odd inputs (kept for compatibility).
    let (sign, rest) = if let Some(rest) = tz.strip_prefix('+') {
        (1, rest)
    } else if let Some(rest) = tz.strip_prefix('-') {
        (-1, rest)
    } else {
        return None;
    };

    let (hours, minutes) = if let Some((hours, minutes)) = rest.split_once(':') {
        (hours, minutes)
    } else if rest.len() == 4 {
        rest.split_at(2)
    } else {
        return None;
    };

    let hours: i32 = hours.parse().ok()?;
    let minutes: i32 = minutes.parse().ok()?;
    if hours > 23 || minutes > 59 {
        return None;
    }

    let total_minutes = hours * 60 + minutes;
    let total_seconds = sign * total_minutes * 60;
    FixedOffset::east_opt(total_seconds)
}

/// Converts a local datetime result to a UTC timestamp in nanoseconds.
///
/// # DST Transition Behavior
///
/// This function handles daylight saving time (DST) transitions by returning an error
/// when the local time is ambiguous or invalid:
///
/// ## Ambiguous Times (Fall Back)
/// When clocks "fall back" (e.g., 2:00 AM becomes 1:00 AM), times in the repeated hour
/// exist twice. For example, in America/New_York on 2024-11-03:
/// - `2024-11-03 01:30:00` occurs both at UTC 05:30 (EDT) and UTC 06:30 (EST)
///
/// DataFusion returns an error rather than silently choosing one interpretation,
/// ensuring users are aware of the ambiguity.
///
/// ## Invalid Times (Spring Forward)
/// When clocks "spring forward" (e.g., 2:00 AM becomes 3:00 AM), times in the skipped hour
/// don't exist. For example, in America/New_York on 2024-03-10:
/// - `2024-03-10 02:30:00` never occurred (clocks jumped from 02:00 to 03:00)
///
/// DataFusion returns an error for these non-existent times.
///
/// ## Workarounds
/// To avoid ambiguity errors:
/// 1. Use timestamps with explicit timezone offsets (e.g., `2024-11-03 01:30:00-05:00`)
/// 2. Convert to UTC before processing
/// 3. Use a timezone without DST (e.g., UTC, `America/Phoenix`)
fn local_datetime_to_timestamp<T: TimeZone>(
    result: LocalResult<DateTime<T>>,
    tz_repr: &str,
) -> Result<i64> {
    match result {
        Single(dt) => datetime_to_timestamp(dt.with_timezone(&Utc)),
        LocalResult::Ambiguous(dt1, dt2) => Err(exec_datafusion_err!(
            "The local time '{:?}' is ambiguous in timezone '{tz_repr}' (also corresponds to '{:?}').",
            dt1.naive_local(),
            dt2.naive_local()
        )),
        LocalResult::None => Err(exec_datafusion_err!(
            "The local time is invalid in timezone '{tz_repr}'."
        )),
    }
}

fn datetime_to_timestamp(datetime: DateTime<Utc>) -> Result<i64> {
    datetime
        .timestamp_nanos_opt()
        .ok_or_else(|| exec_datafusion_err!("{ERR_NANOSECONDS_NOT_SUPPORTED}"))
}

fn timestamp_to_naive(value: i64) -> Result<NaiveDateTime> {
    let secs = value.div_euclid(1_000_000_000);
    let nanos = value.rem_euclid(1_000_000_000) as u32;
    DateTime::<Utc>::from_timestamp(secs, nanos)
        .ok_or_else(|| exec_datafusion_err!("{ERR_NANOSECONDS_NOT_SUPPORTED}"))
        .map(|dt| dt.naive_utc())
}

/// Detects whether a timestamp string contains explicit timezone information.
///
/// This function performs a single-pass scan to check for:
/// 1. RFC3339-compatible format (via Arrow's parser)
/// 2. Timezone offset markers (e.g., `+05:00`, `-0800`, `+05`)
/// 3. Trailing 'Z' or 'z' suffix (UTC indicator)
/// 4. Named timezone identifiers (e.g., `UTC`, `America/New_York`)
///
/// # Performance Considerations
/// This function is called for every string value during timestamp parsing.
/// The implementation uses a single-pass byte-level scan for efficiency.
///
/// # Examples
/// ```ignore
/// assert!(has_explicit_timezone("2020-09-08T13:42:29Z"));
/// assert!(has_explicit_timezone("2020-09-08T13:42:29+05:00"));
/// assert!(has_explicit_timezone("2020-09-08T13:42:29 UTC"));
/// assert!(!has_explicit_timezone("2020-09-08T13:42:29"));
/// ```
/// Heuristic-based explicit timezone detection (simplified).
///
/// Uses chrono for full RFC3339 parsing and a few lightweight heuristics
/// for other common forms (trailing Z, numeric offsets, named tz tokens).
fn has_explicit_timezone(value: &str) -> bool {
    // Fast path: RFC3339 covers many common cases (Z, +HH:MM, etc.)
    if DateTime::parse_from_rfc3339(value).is_ok() {
        return true;
    }

    let v = value.trim();
    if v.ends_with('Z') || v.ends_with('z') {
        return true;
    }

    // Heuristic: trailing numeric offset like +0500, +05:00, -0330, etc.
    if let Some(pos) = v.rfind(|c| c == '+' || c == '-') {
        // Exclude scientific notation like 1.5e+10 (preceded by 'e' or 'E')
        if !(pos > 0
            && v.chars()
                .nth(pos - 1)
                .map(|c| c == 'e' || c == 'E')
                .unwrap_or(false))
        {
            // Ensure the sign likely follows a time component. Look for a separator
            // (space or 'T') before the sign and check for a ':' between that
            // separator and the sign to avoid treating date dashes as offsets.
            let sep_pos = v[..pos].rfind(|c| c == ' ' || c == 'T' || c == 't');
            let has_time_before_sign = if let Some(spos) = sep_pos {
                v[spos + 1..pos].contains(':')
            } else {
                v[..pos].contains(':')
            };

            if has_time_before_sign {
                let rest = &v[pos + 1..];
                let digit_count = rest.chars().take_while(|c| c.is_ascii_digit()).count();
                if digit_count == 2 || digit_count == 4 || digit_count == 6 {
                    return true;
                }
            }
        }
    }

    // Heuristic: named timezones often contain a slash (IANA) or common tokens
    let up = v.to_uppercase();
    if v.contains('/') || up.contains("UTC") || up.contains("GMT") {
        return true;
    }

    // Common abbreviations like PST, EST, etc.
    const COMMON_ABBREVIATIONS: [&str; 8] =
        ["PST", "PDT", "EST", "EDT", "CST", "CDT", "MST", "MDT"];
    for &abbr in COMMON_ABBREVIATIONS.iter() {
        if up.contains(abbr) {
            return true;
        }
    }

    false
}

pub(crate) fn string_to_timestamp_nanos_with_timezone(
    timezone: &ConfiguredTimeZone,
    s: &str,
) -> Result<i64> {
    let ts = string_to_timestamp_nanos_shim(s)?;
    if has_explicit_timezone(s) {
        Ok(ts)
    } else {
        let naive = timestamp_to_naive(ts)?;
        timezone.timestamp_from_naive(&naive)
    }
}

/// Checks that all the arguments from the second are of type [Utf8], [LargeUtf8] or [Utf8View]
///
/// [Utf8]: DataType::Utf8
/// [LargeUtf8]: DataType::LargeUtf8
/// [Utf8View]: DataType::Utf8View
pub(crate) fn validate_data_types(args: &[ColumnarValue], name: &str) -> Result<()> {
    for (idx, a) in args.iter().skip(1).enumerate() {
        match a.data_type() {
            DataType::Utf8View | DataType::LargeUtf8 | DataType::Utf8 => {
                // all good
            }
            _ => {
                return exec_err!(
                    "{name} function unsupported data type at index {}: {}",
                    idx + 1,
                    a.data_type()
                );
            }
        }
    }

    Ok(())
}

/// Accepts a string and parses it using the [`chrono::format::strftime`] specifiers
/// relative to the provided `timezone`
///
/// [IANA timezones] are only supported if the `arrow-array/chrono-tz` feature is enabled
///
/// * `2023-01-01 040506 America/Los_Angeles`
///
/// If a timestamp is ambiguous, for example as a result of daylight-savings time, an error
/// will be returned
///
/// [`chrono::format::strftime`]: https://docs.rs/chrono/latest/chrono/format/strftime/index.html
/// [IANA timezones]: https://www.iana.org/time-zones
pub(crate) fn string_to_datetime_formatted<T: TimeZone>(
    timezone: &T,
    s: &str,
    format: &str,
) -> Result<DateTime<T>, DataFusionError> {
    let err = |err_ctx: &str| {
        exec_datafusion_err!(
            "Error parsing timestamp from '{s}' using format '{format}': {err_ctx}"
        )
    };

    let mut parsed = Parsed::new();
    parse(&mut parsed, s, StrftimeItems::new(format)).map_err(|e| err(&e.to_string()))?;

    // attempt to parse the string assuming it has a timezone
    let dt = parsed.to_datetime();

    if let Err(e) = &dt {
        // no timezone or other failure, try without a timezone
        let ndt = parsed
            .to_naive_datetime_with_offset(0)
            .or_else(|_| parsed.to_naive_date().map(|nd| nd.into()));
        if let Err(e) = &ndt {
            return Err(err(&e.to_string()));
        }

        if let Single(e) = &timezone.from_local_datetime(&ndt.unwrap()) {
            Ok(e.to_owned())
        } else {
            Err(err(&e.to_string()))
        }
    } else {
        Ok(dt.unwrap().with_timezone(timezone))
    }
}

/// Accepts a string with a `chrono` format and converts it to a
/// nanosecond precision timestamp.
///
/// See [`chrono::format::strftime`] for the full set of supported formats.
///
/// Implements the `to_timestamp` function to convert a string to a
/// timestamp, following the model of spark SQL’s to_`timestamp`.
///
/// Internally, this function uses the `chrono` library for the
/// datetime parsing
///
/// ## Timestamp Precision
///
/// Function uses the maximum precision timestamps supported by
/// Arrow (nanoseconds stored as a 64-bit integer) timestamps. This
/// means the range of dates that timestamps can represent is ~1677 AD
/// to 2262 AM
///
/// ## Timezone / Offset Handling
///
/// Numerical values of timestamps are stored compared to offset UTC.
///
/// Any timestamp in the formatting string is handled according to the rules
/// defined by `chrono`.
///
/// [`chrono::format::strftime`]: https://docs.rs/chrono/latest/chrono/format/strftime/index.html
#[inline]
pub(crate) fn string_to_timestamp_nanos_formatted(
    s: &str,
    format: &str,
) -> Result<i64, DataFusionError> {
    string_to_datetime_formatted(&Utc, s, format)?
        .naive_utc()
        .and_utc()
        .timestamp_nanos_opt()
        .ok_or_else(|| exec_datafusion_err!("{ERR_NANOSECONDS_NOT_SUPPORTED}"))
}

pub(crate) fn string_to_timestamp_nanos_formatted_with_timezone(
    timezone: &ConfiguredTimeZone,
    s: &str,
    format: &str,
) -> Result<i64, DataFusionError> {
    if has_explicit_timezone(s) {
        return string_to_timestamp_nanos_formatted(s, format);
    }

    let datetime = timezone.datetime_from_formatted(s, format)?;
    datetime_to_timestamp(datetime)
}

/// Accepts a string with a `chrono` format and converts it to a
/// millisecond precision timestamp.
///
/// See [`chrono::format::strftime`] for the full set of supported formats.
///
/// Internally, this function uses the `chrono` library for the
/// datetime parsing
///
/// ## Timezone / Offset Handling
///
/// Numerical values of timestamps are stored compared to offset UTC.
///
/// Any timestamp in the formatting string is handled according to the rules
/// defined by `chrono`.
///
/// [`chrono::format::strftime`]: https://docs.rs/chrono/latest/chrono/format/strftime/index.html
#[inline]
pub(crate) fn string_to_timestamp_millis_formatted(s: &str, format: &str) -> Result<i64> {
    Ok(string_to_datetime_formatted(&Utc, s, format)?
        .naive_utc()
        .and_utc()
        .timestamp_millis())
}

pub(crate) fn handle<O, F, S>(
    args: &[ColumnarValue],
    op: F,
    name: &str,
) -> Result<ColumnarValue>
where
    O: ArrowPrimitiveType,
    S: ScalarType<O::Native>,
    F: Fn(&str) -> Result<O::Native>,
{
    match &args[0] {
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Utf8View => Ok(ColumnarValue::Array(Arc::new(
                unary_string_to_primitive_function::<&StringViewArray, O, _>(
                    a.as_ref().as_string_view(),
                    op,
                )?,
            ))),
            DataType::LargeUtf8 => Ok(ColumnarValue::Array(Arc::new(
                unary_string_to_primitive_function::<&GenericStringArray<i64>, O, _>(
                    a.as_ref().as_string::<i64>(),
                    op,
                )?,
            ))),
            DataType::Utf8 => Ok(ColumnarValue::Array(Arc::new(
                unary_string_to_primitive_function::<&GenericStringArray<i32>, O, _>(
                    a.as_ref().as_string::<i32>(),
                    op,
                )?,
            ))),
            other => exec_err!("Unsupported data type {other:?} for function {name}"),
        },
        ColumnarValue::Scalar(scalar) => match scalar.try_as_str() {
            Some(a) => {
                let result = a.as_ref().map(|x| op(x)).transpose()?;
                Ok(ColumnarValue::Scalar(S::scalar(result)))
            }
            _ => exec_err!("Unsupported data type {scalar:?} for function {name}"),
        },
    }
}

// Given a function that maps a `&str`, `&str` to an arrow native type,
// returns a `ColumnarValue` where the function is applied to either a `ArrayRef` or `ScalarValue`
// depending on the `args`'s variant.
pub(crate) fn handle_multiple<O, F, S, M>(
    args: &[ColumnarValue],
    op: F,
    op2: M,
    name: &str,
) -> Result<ColumnarValue>
where
    O: ArrowPrimitiveType,
    S: ScalarType<O::Native>,
    F: Fn(&str, &str) -> Result<O::Native>,
    M: Fn(O::Native) -> O::Native,
{
    match &args[0] {
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Utf8View | DataType::LargeUtf8 | DataType::Utf8 => {
                // validate the column types
                for (pos, arg) in args.iter().enumerate() {
                    match arg {
                        ColumnarValue::Array(arg) => match arg.data_type() {
                            DataType::Utf8View | DataType::LargeUtf8 | DataType::Utf8 => {
                                // all good
                            }
                            other => return exec_err!("Unsupported data type {other:?} for function {name}, arg # {pos}"),
                        },
                        ColumnarValue::Scalar(arg) => {
                            match arg.data_type() {
                                DataType::Utf8View| DataType::LargeUtf8 | DataType::Utf8 => {
                                    // all good
                                }
                                other => return exec_err!("Unsupported data type {other:?} for function {name}, arg # {pos}"),
                            }
                        }
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(
                    strings_to_primitive_function::<O, _, _>(args, op, op2, name)?,
                )))
            }
            other => {
                exec_err!("Unsupported data type {other:?} for function {name}")
            }
        },
        // if the first argument is a scalar utf8 all arguments are expected to be scalar utf8
        ColumnarValue::Scalar(scalar) => match scalar.try_as_str() {
            Some(a) => {
                let a = a.as_ref();
                // ASK: Why do we trust `a` to be non-null at this point?
                let a = unwrap_or_internal_err!(a);

                let mut ret = None;

                for (pos, v) in args.iter().enumerate().skip(1) {
                    let ColumnarValue::Scalar(
                        ScalarValue::Utf8View(x)
                        | ScalarValue::LargeUtf8(x)
                        | ScalarValue::Utf8(x),
                    ) = v
                    else {
                        return exec_err!("Unsupported data type {v:?} for function {name}, arg # {pos}");
                    };

                    if let Some(s) = x {
                        match op(a, s.as_str()) {
                            Ok(r) => {
                                ret = Some(Ok(ColumnarValue::Scalar(S::scalar(Some(
                                    op2(r),
                                )))));
                                break;
                            }
                            Err(e) => ret = Some(Err(e)),
                        }
                    }
                }

                unwrap_or_internal_err!(ret)
            }
            other => {
                exec_err!("Unsupported data type {other:?} for function {name}")
            }
        },
    }
}

/// given a function `op` that maps `&str`, `&str` to the first successful Result
/// of an arrow native type, returns a `PrimitiveArray` after the application of the
/// function to `args` and the subsequence application of the `op2` function to any
/// successful result. This function calls the `op` function with the first and second
/// argument and if not successful continues with first and third, first and fourth,
/// etc until the result was successful or no more arguments are present.
/// # Errors
/// This function errors iff:
/// * the number of arguments is not > 1 or
/// * the function `op` errors for all input
pub(crate) fn strings_to_primitive_function<O, F, F2>(
    args: &[ColumnarValue],
    op: F,
    op2: F2,
    name: &str,
) -> Result<PrimitiveArray<O>>
where
    O: ArrowPrimitiveType,
    F: Fn(&str, &str) -> Result<O::Native>,
    F2: Fn(O::Native) -> O::Native,
{
    if args.len() < 2 {
        return exec_err!(
            "{:?} args were supplied but {} takes 2 or more arguments",
            args.len(),
            name
        );
    }

    match &args[0] {
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Utf8View => {
                let string_array = a.as_string_view();
                handle_array_op::<O, &StringViewArray, F, F2>(
                    &string_array,
                    &args[1..],
                    op,
                    op2,
                )
            }
            DataType::LargeUtf8 => {
                let string_array = as_generic_string_array::<i64>(&a)?;
                handle_array_op::<O, &GenericStringArray<i64>, F, F2>(
                    &string_array,
                    &args[1..],
                    op,
                    op2,
                )
            }
            DataType::Utf8 => {
                let string_array = as_generic_string_array::<i32>(&a)?;
                handle_array_op::<O, &GenericStringArray<i32>, F, F2>(
                    &string_array,
                    &args[1..],
                    op,
                    op2,
                )
            }
            other => exec_err!(
                "Unsupported data type {other:?} for function substr,\
                    expected Utf8View, Utf8 or LargeUtf8."
            ),
        },
        other => exec_err!(
            "Received {} data type, expected only array",
            other.data_type()
        ),
    }
}

fn handle_array_op<'a, O, V, F, F2>(
    first: &V,
    args: &[ColumnarValue],
    op: F,
    op2: F2,
) -> Result<PrimitiveArray<O>>
where
    V: StringArrayType<'a>,
    O: ArrowPrimitiveType,
    F: Fn(&str, &str) -> Result<O::Native>,
    F2: Fn(O::Native) -> O::Native,
{
    first
        .iter()
        .enumerate()
        .map(|(pos, x)| {
            let mut val = None;
            if let Some(x) = x {
                for arg in args {
                    let v = match arg {
                        ColumnarValue::Array(a) => match a.data_type() {
                            DataType::Utf8View => Ok(a.as_string_view().value(pos)),
                            DataType::LargeUtf8 => Ok(a.as_string::<i64>().value(pos)),
                            DataType::Utf8 => Ok(a.as_string::<i32>().value(pos)),
                            other => exec_err!("Unexpected type encountered '{other}'"),
                        },
                        ColumnarValue::Scalar(s) => match s.try_as_str() {
                            Some(Some(v)) => Ok(v),
                            Some(None) => continue, // null string
                            None => exec_err!("Unexpected scalar type encountered '{s}'"),
                        },
                    }?;

                    let r = op(x, v);
                    if let Ok(inner) = r {
                        val = Some(Ok(op2(inner)));
                        break;
                    } else {
                        val = Some(r);
                    }
                }
            };

            val.transpose()
        })
        .collect()
}

/// given a function `op` that maps a `&str` to a Result of an arrow native type,
/// returns a `PrimitiveArray` after the application
/// of the function to `args[0]`.
/// # Errors
/// This function errors iff:
/// * the number of arguments is not 1 or
/// * the function `op` errors
fn unary_string_to_primitive_function<'a, StringArrType, O, F>(
    array: StringArrType,
    op: F,
) -> Result<PrimitiveArray<O>>
where
    StringArrType: StringArrayType<'a>,
    O: ArrowPrimitiveType,
    F: Fn(&'a str) -> Result<O::Native>,
{
    // first map is the iterator, second is for the `Option<_>`
    array.iter().map(|x| x.map(&op).transpose()).collect()
}

#[cfg(test)]
mod tests {
    use super::{has_explicit_timezone, ConfiguredTimeZone};
    use datafusion_common::config::ConfigOptions;

    #[test]
    fn parse_empty_timezone_returns_none() {
        assert!(ConfiguredTimeZone::parse("   ").unwrap().is_none());
    }

    #[test]
    fn from_config_blank_timezone_defaults_to_utc() {
        let mut config = ConfigOptions::default();
        config.execution.time_zone = None;

        let timezone = ConfiguredTimeZone::from_config(&config);
        assert_eq!(timezone, ConfiguredTimeZone::utc());
    }

    #[test]
    fn detects_timezone_token_outside_tail() {
        assert!(has_explicit_timezone("UTC 2024-01-01 12:00:00"));
        assert!(has_explicit_timezone("2020-09-08T13:42:29UTC"));
        assert!(has_explicit_timezone("America/New_York 2020-09-08"));
    }

    #[test]
    fn detects_offsets_without_colons() {
        // ISO-8601 formats with offsets (no colons)
        assert!(has_explicit_timezone("2020-09-08T13:42:29+0500"));
        assert!(has_explicit_timezone("2020-09-08T13:42:29-0330"));
        assert!(has_explicit_timezone("2020-09-08T13:42:29+05"));
        assert!(has_explicit_timezone("2020-09-08T13:42:29-08"));

        // 4-digit offsets
        assert!(has_explicit_timezone("2024-01-01T12:00:00+0000"));
        assert!(has_explicit_timezone("2024-01-01T12:00:00-1200"));

        // 6-digit offsets (with seconds)
        assert!(has_explicit_timezone("2024-01-01T12:00:00+053045"));
        assert!(has_explicit_timezone("2024-01-01T12:00:00-123045"));

        // Lowercase 't' separator
        assert!(has_explicit_timezone("2020-09-08t13:42:29+0500"));
        assert!(has_explicit_timezone("2020-09-08t13:42:29-0330"));
    }

    #[test]
    fn detects_offsets_with_colons() {
        assert!(has_explicit_timezone("2020-09-08T13:42:29+05:00"));
        assert!(has_explicit_timezone("2020-09-08T13:42:29-03:30"));
        assert!(has_explicit_timezone("2020-09-08T13:42:29+05:00:45"));
    }

    #[test]
    fn detects_z_suffix() {
        assert!(has_explicit_timezone("2020-09-08T13:42:29Z"));
        assert!(has_explicit_timezone("2020-09-08T13:42:29z"));
    }

    #[test]
    fn rejects_naive_timestamps() {
        assert!(!has_explicit_timezone("2020-09-08T13:42:29"));
        assert!(!has_explicit_timezone("2020-09-08 13:42:29"));
        assert!(!has_explicit_timezone("2024-01-01 12:00:00"));

        // Date formats with dashes that could be confused with offsets
        assert!(!has_explicit_timezone("03:59:00.123456789 05-17-2023"));
        assert!(!has_explicit_timezone("12:00:00 01-02-2024"));
    }

    #[test]
    fn rejects_scientific_notation() {
        // Should not treat scientific notation as timezone offset
        assert!(!has_explicit_timezone("1.5e+10"));
        assert!(!has_explicit_timezone("2.3E-05"));
    }

    #[test]
    fn test_offset_without_colon_parsing() {
        use super::{
            string_to_timestamp_nanos_shim, string_to_timestamp_nanos_with_timezone,
        };

        // Test the exact case from the issue: 2020-09-08T13:42:29+0500
        // This should parse correctly as having an explicit offset
        let utc_tz = ConfiguredTimeZone::parse("UTC")
            .unwrap()
            .expect("UTC should parse");
        let result_utc =
            string_to_timestamp_nanos_with_timezone(&utc_tz, "2020-09-08T13:42:29+0500")
                .unwrap();

        // Parse the equivalent RFC3339 format with colon to get the expected value
        let expected =
            string_to_timestamp_nanos_shim("2020-09-08T13:42:29+05:00").unwrap();
        assert_eq!(result_utc, expected);

        // Test with America/New_York timezone - should NOT double-adjust
        // Because the timestamp has an explicit timezone, the session timezone should be ignored
        let ny_tz = ConfiguredTimeZone::parse("America/New_York")
            .unwrap()
            .expect("America/New_York should parse");
        let result_ny =
            string_to_timestamp_nanos_with_timezone(&ny_tz, "2020-09-08T13:42:29+0500")
                .unwrap();

        // The result should be the same as UTC because the timestamp has an explicit timezone
        assert_eq!(result_ny, expected);

        // Test other offset formats without colons
        let result2 =
            string_to_timestamp_nanos_with_timezone(&utc_tz, "2020-09-08T13:42:29-0330")
                .unwrap();
        let expected2 =
            string_to_timestamp_nanos_shim("2020-09-08T13:42:29-03:30").unwrap();
        assert_eq!(result2, expected2);

        // Test 2-digit offsets
        let result3 =
            string_to_timestamp_nanos_with_timezone(&utc_tz, "2020-09-08T13:42:29+05")
                .unwrap();
        let expected3 =
            string_to_timestamp_nanos_shim("2020-09-08T13:42:29+05:00").unwrap();
        assert_eq!(result3, expected3);
    }

    #[test]
    fn detects_named_timezones_with_trailing_offsets() {
        use super::has_explicit_timezone;

        // Named timezone tokens and trailing offsets should be detected by the
        // simplified explicit-timezone heuristic.
        assert!(has_explicit_timezone("America/Los_Angeles+8"));
        assert!(has_explicit_timezone("PST+8"));
        assert!(has_explicit_timezone("Europe/London-1"));
        assert!(has_explicit_timezone("UTC+0"));

        assert!(has_explicit_timezone(
            "2024-01-01T12:00:00 Europe/London+05"
        ));
        assert!(has_explicit_timezone("Meeting at 12:00 PST+8"));
        assert!(has_explicit_timezone("Event at 12:00 Europe/London-1"));
    }
}
