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

use datafusion_common::DataFusionError;
use datafusion_common::config::ConfigOptions;
use dyn_eq::DynEq;
use dyn_hash::DynHash;
use std::fmt::Debug;

/// A trait for parsing timestamps from strings. Two implementations are provided:
/// `ChronoDateTimeParser` (the default) which uses [Chrono] to parse timestamps, and
/// `JiffDateTimeParser` (via the `jiff` feature flag) which uses [Jiff] to parse
/// timestamps.
///
/// While both implementations are capable of parsing timestamps, the `ChronoDateTimeParser`
/// is a bit more lenient wrt formats at the cost of being slightly slower than Jiff.
/// Jiff, on the other hand, has slightly less support for some format options than Chrono
/// but is measurably faster than Chrono in some situations.
///
/// [Chrono]: https://docs.rs/chrono/latest/chrono/
/// [Jiff]: https://docs.rs/jiff/latest/jiff/
pub trait DateTimeParser: Debug + DynEq + DynHash + Send + Sync {
    fn string_to_timestamp_nanos(
        &self,
        tz: &str,
        s: &str,
    ) -> datafusion_common::Result<i64, DataFusionError>;

    fn string_to_timestamp_nanos_formatted(
        &self,
        tz: &str,
        s: &str,
        formats: &[&str],
    ) -> datafusion_common::Result<i64, DataFusionError>;

    fn string_to_timestamp_millis_formatted(
        &self,
        tz: &str,
        s: &str,
        formats: &[&str],
    ) -> datafusion_common::Result<i64, DataFusionError>;
}

// impl Eq and PartialEaq for dyn DateTimeParser
dyn_eq::eq_trait_object!(DateTimeParser);

// Implement std::hash::Hash for dyn DateTimeParser
dyn_hash::hash_trait_object!(DateTimeParser);

pub fn get_date_time_parser(config_options: &ConfigOptions) -> Box<dyn DateTimeParser> {
    let parser_cfg = config_options.execution.date_time_parser.as_ref();

    match parser_cfg {
        Some(p) => match p.as_str() {
            "chrono" => {
                Box::new(chrono::ChronoDateTimeParser::new()) as Box<dyn DateTimeParser>
            }
            #[cfg(feature = "jiff")]
            "jiff" => {
                Box::new(jiff::JiffDateTimeParser::new()) as Box<dyn DateTimeParser>
            }
            _ => panic!("Unknown/unsupported date time parser: {p}"),
        },
        None => Box::new(chrono::ChronoDateTimeParser::new()) as Box<dyn DateTimeParser>,
    }
}

pub mod chrono {
    use crate::datetime::parser::DateTimeParser;
    use arrow::array::timezone::Tz;
    use arrow::compute::kernels::cast_utils::string_to_datetime;
    use chrono::LocalResult;
    use chrono::format::{ParseErrorKind, Parsed, StrftimeItems, parse};
    use chrono_tz::ParseError;
    use datafusion_common::{DataFusionError, exec_datafusion_err, exec_err};

    const ERR_NANOSECONDS_NOT_SUPPORTED: &str = "The dates that can be represented as nanoseconds have to be between 1677-09-21T00:12:44.0 and 2262-04-11T23:47:16.854775804";

    #[derive(Debug, Default, PartialEq, Eq, Hash)]
    pub struct ChronoDateTimeParser {}

    impl ChronoDateTimeParser {
        pub fn new() -> Self {
            Self {}
        }

        /// Accepts a string and parses it using the [`chrono::format::strftime`] specifiers
        /// relative to the provided `timezone`.
        ///
        /// If a timestamp is ambiguous, for example, as a result of daylight-savings time, an error
        /// will be returned.
        ///
        /// [`chrono::format::strftime`]: https://docs.rs/chrono/latest/chrono/format/strftime/index.html
        fn string_to_datetime_formatted<T: chrono::TimeZone>(
            &self,
            timezone: &T,
            s: &str,
            formats: &[&str],
        ) -> Result<chrono::DateTime<T>, DataFusionError> {
            let mut err: Option<String> = None;

            for &format in formats.iter() {
                let mut format = format;
                let mut datetime_str = s;

                // we manually handle the most common case of a named timezone at the end of the timestamp
                // since chrono doesn't support that directly. Note, however, that %+ does handle 'Z' at the
                // end of the string.
                //
                // This code doesn't handle named timezones with no preceding space since that would require
                // writing a custom parser.
                let tz: Option<chrono_tz::Tz> = if format.ends_with(" %Z") {
                    // grab the string after the last space as the named timezone
                    let parts: Vec<&str> = datetime_str.rsplitn(2, ' ').collect();
                    let timezone_name = parts[0];
                    datetime_str = parts[1];

                    // attempt to parse the timezone name
                    let result: Result<chrono_tz::Tz, ParseError> = timezone_name.parse();
                    let Ok(tz) = result else {
                        err = Some(result.unwrap_err().to_string());
                        continue;
                    };

                    // successfully parsed the timezone name, remove the ' %Z' from the format
                    format = &format[..format.len() - 3];

                    Some(tz)
                } else if format.contains("%Z") {
                    err = Some("'%Z' is only supported at the end of the format string preceded by a space".into());
                    continue;
                } else {
                    None
                };

                let mut parsed = Parsed::new();
                let items = StrftimeItems::new(format);
                let result = parse(&mut parsed, datetime_str, items);

                if let Err(e) = &result {
                    err = Some(e.to_string());
                    continue;
                }

                let dt = match tz {
                    Some(tz) => {
                        // A timezone was manually parsed out, convert it to a fixed offset
                        match parsed.to_datetime_with_timezone(&tz) {
                            Ok(dt) => Ok(dt.fixed_offset()),
                            Err(e) => Err(e),
                        }
                    }
                    // default to parse the string assuming it has a timezone
                    None => parsed.to_datetime(),
                };

                match dt {
                    Ok(dt) => return Ok(dt.with_timezone(timezone)),
                    Err(e) if e.kind() == ParseErrorKind::Impossible => {
                        err = Some(format!(
                            "Unable to parse timestamp {datetime_str} in timezone {tz:?}: datetime was impossible"
                        ));
                        continue;
                    }
                    Err(e) if e.kind() == ParseErrorKind::OutOfRange => {
                        err = Some(format!(
                            "Unable to parse timestamp {datetime_str} in timezone {tz:?}: datetime was out of range"
                        ));
                        continue;
                    }
                    _ => {
                        // no timezone or other failure, try without a timezone
                        let ndt = parsed
                            .to_naive_datetime_with_offset(0)
                            .or_else(|_| parsed.to_naive_date().map(|nd| nd.into()));
                        if let Err(e) = &ndt {
                            err = Some(e.to_string());
                            continue;
                        }

                        let result = &timezone.from_local_datetime(&ndt.unwrap());

                        match result {
                            LocalResult::Single(e) => return Ok(e.to_owned()),
                            LocalResult::None => {
                                err = Some(format!(
                                    "Unable to parse timestamp {datetime_str} in timezone {tz:?}: no valid local time found"
                                ));
                                continue;
                            }
                            LocalResult::Ambiguous(earliest, latest) => {
                                err = Some(format!(
                                    "Unable to parse timestamp {datetime_str} in timezone {tz:?}: ambiguous timestamps found {earliest:?} and {latest:?}"
                                ));
                                continue;
                            }
                        }
                    }
                }
            }

            match err {
                Some(e) => exec_err!(
                    "Error parsing timestamp from '{s}' using formats: {formats:?}: {e}"
                ),
                None => exec_err!(
                    "Error parsing timestamp from '{s}' using formats: {formats:?}"
                ),
            }
        }
    }

    impl DateTimeParser for ChronoDateTimeParser {
        /// Accepts a string and parses it relative to the provided `timezone`
        ///
        /// In addition to RFC3339 / ISO8601 standard timestamps, it also
        /// accepts strings that use a space ` ` to separate the date and time
        /// as well as strings that have no explicit timezone offset.
        ///
        /// Examples of accepted inputs:
        /// * `1997-01-31T09:26:56.123Z`        # RCF3339
        /// * `1997-01-31T09:26:56.123-05:00`   # RCF3339
        /// * `1997-01-31 09:26:56.123-05:00`   # close to RCF3339 but with a space rather than T
        /// * `2023-01-01 04:05:06.789 -08`     # close to RCF3339, no fractional seconds or time separator
        /// * `1997-01-31T09:26:56.123`         # close to RCF3339 but no timezone offset specified
        /// * `1997-01-31 09:26:56.123`         # close to RCF3339 but uses a space and no timezone offset
        /// * `1997-01-31 09:26:56`             # close to RCF3339, no fractional seconds
        /// * `1997-01-31 092656`               # close to RCF3339, no fractional seconds
        /// * `1997-01-31 092656+04:00`         # close to RCF3339, no fractional seconds or time separator
        /// * `1997-01-31`                      # close to RCF3339, only date no time
        ///
        /// [IANA timezones] are only supported if the `arrow-array/chrono-tz` feature is enabled
        ///
        /// * `2023-01-01 040506 America/Los_Angeles`
        ///
        /// If a timestamp is ambiguous, for example as a result of daylight-savings time, an error
        /// will be returned
        ///
        /// Some formats supported by PostgresSql <https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-DATETIME-TIME-TABLE>
        /// are not supported, like
        ///
        /// * "2023-01-01 04:05:06.789 +07:30:00",
        /// * "2023-01-01 040506 +07:30:00",
        /// * "2023-01-01 04:05:06.789 PST",
        #[inline]
        fn string_to_timestamp_nanos(
            &self,
            tz: &str,
            s: &str,
        ) -> Result<i64, DataFusionError> {
            let tz: Tz = match tz.parse() {
                Ok(tz) => tz,
                Err(e) => return exec_err!("Invalid timezone '{tz}': {e}"),
            };

            let dt = string_to_datetime(&tz, s)?;
            let parsed = dt
                .timestamp_nanos_opt()
                .ok_or_else(|| exec_datafusion_err!("{ERR_NANOSECONDS_NOT_SUPPORTED}"))?;

            Ok(parsed)
        }

        /// Accepts a string with an array of `chrono` formats and converts it to a
        /// nanosecond precision timestamp according to the rules
        /// defined by `chrono`.
        ///
        /// See [`chrono::format::strftime`] for the full set of supported formats.
        ///
        /// ## Timestamp Precision
        ///
        /// This function uses the maximum precision timestamps supported by
        /// Arrow (nanoseconds stored as a 64-bit integer) timestamps. This
        /// means the range of dates that timestamps can represent is ~1677 AD
        /// to 2262 AM.
        ///
        /// ## Timezone / Offset Handling
        ///
        /// Numerical values of timestamps are stored compared to offset UTC.
        ///
        /// Note that parsing named [IANA timezones] is not supported yet in chrono
        /// <https://github.com/chronotope/chrono/issues/38> and this implementation
        /// only supports named timezones at the end of the string preceded by a space.
        ///
        /// [`chrono::format::strftime`]: https://docs.rs/chrono/latest/chrono/format/strftime/index.html
        /// [IANA timezones]: https://www.iana.org/time-zones
        #[inline]
        fn string_to_timestamp_nanos_formatted(
            &self,
            tz: &str,
            s: &str,
            formats: &[&str],
        ) -> Result<i64, DataFusionError> {
            let tz: Tz = match tz.parse() {
                Ok(tz) => tz,
                Err(e) => return exec_err!("Invalid timezone '{tz}': {e}"),
            };

            let dt = self.string_to_datetime_formatted(&tz, s, formats)?;
            dt.naive_utc()
                .and_utc()
                .timestamp_nanos_opt()
                .ok_or_else(|| exec_datafusion_err!("{ERR_NANOSECONDS_NOT_SUPPORTED}"))
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
        fn string_to_timestamp_millis_formatted(
            &self,
            tz: &str,
            s: &str,
            formats: &[&str],
        ) -> Result<i64, DataFusionError> {
            let tz: Tz = match tz.parse() {
                Ok(tz) => tz,
                Err(e) => return exec_err!("Invalid timezone '{tz}': {e}"),
            };

            Ok(self
                .string_to_datetime_formatted(&tz, s, formats)?
                .naive_utc()
                .and_utc()
                .timestamp_millis())
        }
    }
}

#[cfg(feature = "jiff")]
pub mod jiff {
    use crate::datetime::parser::DateTimeParser;
    use arrow::array::timezone::Tz;
    use arrow::error::ArrowError;
    use chrono::format::{Parsed, StrftimeItems, parse};
    use datafusion_common::{DataFusionError, exec_datafusion_err, exec_err};
    use jiff::civil::Time;
    use jiff::fmt::temporal::Pieces;
    use jiff::tz::{Disambiguation, TimeZone};
    use jiff::{Timestamp, Zoned};
    use num_traits::ToPrimitive;

    const ERR_NANOSECONDS_NOT_SUPPORTED: &str = "The dates that can be represented as nanoseconds have to be between 1677-09-21T00:12:44.0 and 2262-04-11T23:47:16.854775804";

    #[derive(Debug, PartialEq, Eq, Hash)]
    pub struct JiffDateTimeParser {}

    impl JiffDateTimeParser {
        pub fn new() -> Self {
            Self {}
        }

        /// Attempts to parse a given string representation of a timestamp into a `Timestamp` object.
        /// The function also adjusts the datetime for the specified timezone.
        ///
        /// # Parameters
        /// - `timezone`: A reference to the `TimeZone` object used to adjust the parsed datetime to the desired timezone.
        /// - `s`: A string slice holding the timestamp to be parsed.
        ///
        /// # Returns
        /// - `Ok(Timestamp)`: Contains the parsed timestamp in seconds since the Unix epoch.
        /// - `Err(ArrowError)`: Returned in the event of errors in parsing
        ///   the timestamp string or computing the timezone offset.
        ///
        /// # Errors
        /// This function will return an `ArrowError` if:
        ///
        /// - The string `s` is shorter than 10 characters.
        /// - The format of the string does not match expected timestamp patterns.
        /// - An invalid/unknown time zone or offset is provided during parsing.
        /// - Errors occur while converting the datetime to a specific time zone.
        fn string_to_datetime(
            &self,
            timezone: &TimeZone,
            s: &str,
        ) -> datafusion_common::Result<Timestamp, ArrowError> {
            let err = |ctx: &str| {
                ArrowError::ParseError(format!(
                    "Error parsing timestamp from '{s}': {ctx}"
                ))
            };

            let bytes = s.as_bytes();
            if bytes.len() < 10 {
                return Err(err("timestamp must contain at least 10 characters"));
            }

            let pieces = Pieces::parse(bytes).map_err(|e| err(&format!("{e:?}")))?;
            let time = pieces.time().unwrap_or_else(Time::midnight);
            let dt = pieces.date().to_datetime(time);
            let tz = match pieces
                .to_time_zone()
                .map_err(|e| err(&format!("unknown time zone: {e:?}")))?
            {
                Some(tz) => tz,
                None => match pieces.to_numeric_offset() {
                    Some(offset) => TimeZone::fixed(offset),
                    None => timezone.to_owned(),
                },
            };
            let zdt = tz
                .to_zoned(dt)
                .map_err(|e| err(&format!("error computing timezone offset: {e:?}")))?;

            Ok(zdt.timestamp())
        }

        /// Attempts to parse a given string representation of a timestamp into a `Zoned` datetime object
        /// using a list of provided formats. The function also adjusts the datetime for the specified timezone.
        ///
        /// # Parameters
        /// - `timezone`: A reference to the `TimeZone` object used to adjust the parsed datetime to the desired timezone.
        /// - `s`: A string slice holding the timestamp to be parsed.
        /// - `formats`: A slice of string slices representing the accepted datetime formats to use when parsing.
        ///
        /// # Returns
        /// - `Ok(Zoned)` if the string is successfully parsed into a `Zoned` datetime object using one of the formats provided.
        /// - `Err(DataFusionError)` if the string cannot be parsed or if there are issues related to the timezone or format handling.
        ///
        /// # Behavior
        /// 1. Iterates through the list of provided formats.
        /// 2. Handles special cases such as `%Z` (timezone) or `%c` (locale-aware datetime representation)
        ///    by either modifying the format string or switching to different parsing logic.
        /// 3. Attempts to resolve timezone-related issues:
        ///     - Calculates a fixed offset for the given `timezone`.
        ///     - Handles attempts to parse timezones using IANA names or offsets.
        /// 4. If parsing fails for the current format, the function moves to the next format and aggregates any parsing errors.
        /// 5. If no formats succeed, an error is returned summarizing the failure.
        ///
        /// # Errors
        /// - Returns a descriptive error wrapped in `DataFusionError` if:
        ///   - The input string is not compatible with any of the provided formats.
        ///   - There are issues parsing the timezone from the input string or adjusting to the provided `TimeZone`.
        ///   - There are issues resolving ambiguous datetime representations.
        fn string_to_datetime_formatted(
            &self,
            timezone: &TimeZone,
            s: &str,
            formats: &[&str],
        ) -> datafusion_common::Result<Zoned, DataFusionError> {
            let mut err: Option<String> = None;

            for &format in formats.iter() {
                let mut format = if format.contains("%Z") {
                    &format.replace("%Z", "%Q")
                } else {
                    format
                };

                if format == "%c" {
                    // switch to chrono, jiff doesn't support parsing with %c
                    let mut parsed = Parsed::new();
                    let result = parse(&mut parsed, s, StrftimeItems::new(format));

                    if result.is_err() {
                        err = Some(format!("error parsing timestamp: {result:?}"));
                        continue;
                    }

                    let offset = timezone.to_fixed_offset();
                    let tz: &str = if let Ok(offset) = offset {
                        &offset.to_string()
                    } else {
                        let result = timezone.iana_name();
                        match result {
                            Some(tz) => tz,
                            None => {
                                err =
                                    Some(format!("error parsing timezone: {timezone:?}"));
                                continue;
                            }
                        }
                    };
                    let tz: Tz = tz.parse::<Tz>().ok().unwrap_or("UTC".parse::<Tz>()?);

                    match parsed.to_datetime_with_timezone(&tz) {
                        Ok(dt) => {
                            let dt = dt.fixed_offset();
                            let result =
                                Timestamp::from_nanosecond(dt.timestamp() as i128);
                            match result {
                                Ok(result) => {
                                    return Ok(result.to_zoned(timezone.to_owned()));
                                }
                                Err(e) => {
                                    err = Some(e.to_string());
                                    continue;
                                }
                            }
                        }
                        Err(e) => {
                            err = Some(e.to_string());
                            continue;
                        }
                    };
                }

                let result = if format == "%+" {
                    // jiff doesn't support parsing with %+, but the equivalent of %+ is
                    // somewhat rfc3389 which is what is used by the parse method
                    let result: Result<Zoned, _> = s.parse();

                    if let Ok(r) = result {
                        return Ok(r);
                    } else {
                        // however, the above only works with timezone names, not offsets
                        let s = if s.ends_with("Z") {
                            &(s.trim_end_matches("Z").to_owned() + "+00:00")
                        } else {
                            s
                        };

                        // try again with fractional seconds
                        format = "%Y-%m-%dT%H:%M:%S%.f%:z";

                        jiff::fmt::strtime::parse(format, s)
                    }
                } else {
                    jiff::fmt::strtime::parse(format, s)
                };

                match result {
                    Ok(bdt) => {
                        if bdt.iana_time_zone().is_some() || bdt.offset().is_some() {
                            if let Ok(zoned) = bdt.to_zoned() {
                                return Ok(zoned.with_time_zone(timezone.to_owned()));
                            }
                        }

                        let result = bdt.to_datetime();
                        let datetime = match result {
                            Ok(datetime) => datetime,
                            Err(e) => {
                                err = Some(e.to_string());
                                continue;
                            }
                        };

                        let zoned = timezone
                            .to_owned()
                            .into_ambiguous_zoned(datetime)
                            .disambiguate(Disambiguation::Compatible);

                        match zoned {
                            Ok(z) => return Ok(z),
                            Err(e) => {
                                err = Some(e.to_string());
                                continue;
                            }
                        }
                    }
                    Err(e) => {
                        err = Some(e.to_string());
                        continue;
                    }
                }
            }

            match err {
                Some(e) => exec_err!("{e}"),
                None => {
                    exec_err!("Unable to parse timestamp: {s} with formats: {formats:?}")
                }
            }
        }
    }

    impl DateTimeParser for JiffDateTimeParser {
        /// Accepts a string with a `jiff` format and converts it to a
        /// nanosecond precision timestamp according to the rules
        /// defined by `jiff`.
        ///
        /// See <https://docs.rs/jiff/latest/jiff/fmt/strtime/index.html#conversion-specifications>
        /// for the full set of supported formats.
        ///
        /// ## Timestamp Precision
        ///
        /// This function uses the maximum precision timestamps supported by
        /// Arrow (nanoseconds stored as a 64-bit integer) timestamps. This
        /// means the range of dates that timestamps can represent is ~1677 AD
        /// to 2262 AM.
        ///
        /// ## Timezone / Offset Handling
        ///
        /// Numerical values of timestamps are stored compared to offset UTC.
        #[inline]
        fn string_to_timestamp_nanos(
            &self,
            tz: &str,
            s: &str,
        ) -> Result<i64, DataFusionError> {
            let result = TimeZone::get(tz.as_ref());
            let timezone = match result {
                Ok(tz) => tz,
                Err(e) => return exec_err!("Invalid timezone {tz}: {e}"),
            };

            let timestamp = self.string_to_datetime(&timezone, s)?;
            let parsed = timestamp
                .as_nanosecond()
                .to_i64()
                .ok_or_else(|| exec_datafusion_err!("{ERR_NANOSECONDS_NOT_SUPPORTED}"))?;

            Ok(parsed)
        }

        /// Accepts a string with an array of `jiff` formats and converts it to a
        /// nanosecond precision timestamp according to the rules
        /// defined by `jiff`.
        ///
        /// See <https://docs.rs/jiff/latest/jiff/fmt/strtime/index.html#conversion-specifications>
        /// for the full set of supported formats.
        ///
        /// ## Timestamp Precision
        ///
        /// This function uses the maximum precision timestamps supported by
        /// Arrow (nanoseconds stored as a 64-bit integer) timestamps. This
        /// means the range of dates that timestamps can represent is ~1677 AD
        /// to 2262 AM.
        ///
        /// ## Timezone / Offset Handling
        ///
        /// Numerical values of timestamps are stored compared to offset UTC.
        #[inline]
        fn string_to_timestamp_nanos_formatted(
            &self,
            tz: &str,
            s: &str,
            formats: &[&str],
        ) -> Result<i64, DataFusionError> {
            let result = TimeZone::get(tz.as_ref());
            let timezone = match result {
                Ok(tz) => tz,
                Err(e) => return exec_err!("Invalid timezone {tz}: {e}"),
            };

            let zoned = self.string_to_datetime_formatted(&timezone, s, formats)?;
            let parsed =
                zoned.timestamp().as_nanosecond().to_i64().ok_or_else(|| {
                    exec_datafusion_err!("{ERR_NANOSECONDS_NOT_SUPPORTED}")
                })?;

            Ok(parsed)
        }

        /// Accepts a string with an array of `jiff` formats and converts it to a
        /// millisecond precision timestamp according to the rules
        /// defined by `jiff`.
        ///
        /// See <https://docs.rs/jiff/latest/jiff/fmt/strtime/index.html#conversion-specifications>
        /// for the full set of supported formats.
        ///
        /// ## Timezone / Offset Handling
        ///
        /// Numerical values of timestamps are stored compared to offset UTC.
        #[inline]
        fn string_to_timestamp_millis_formatted(
            &self,
            tz: &str,
            s: &str,
            formats: &[&str],
        ) -> Result<i64, DataFusionError> {
            let result = TimeZone::get(tz.as_ref());
            let timezone = match result {
                Ok(tz) => tz,
                Err(e) => return exec_err!("Invalid timezone {tz}: {e}"),
            };

            let dt = self.string_to_datetime_formatted(&timezone, s, formats)?;
            let parsed = dt.timestamp().as_millisecond();

            Ok(parsed)
        }
    }
}
