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

use std::fmt::{self, Display};
use std::str::FromStr;

use crate::config::{ConfigField, Visit};
use crate::error::{DataFusionError, Result};

/// Parquet writer version options for controlling the Parquet file format version
///
/// This enum validates parquet writer version values at configuration time,
/// ensuring only valid versions ("1.0" or "2.0") can be set via `SET` commands
/// or proto deserialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DFParquetWriterVersion {
    /// Parquet format version 1.0
    #[default]
    V1_0,
    /// Parquet format version 2.0
    V2_0,
}

/// Implement parsing strings to `DFParquetWriterVersion`
impl FromStr for DFParquetWriterVersion {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "1.0" => Ok(DFParquetWriterVersion::V1_0),
            "2.0" => Ok(DFParquetWriterVersion::V2_0),
            other => Err(DataFusionError::Configuration(format!(
                "Invalid parquet writer version: {other}. Expected one of: 1.0, 2.0"
            ))),
        }
    }
}

impl Display for DFParquetWriterVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            DFParquetWriterVersion::V1_0 => "1.0",
            DFParquetWriterVersion::V2_0 => "2.0",
        };
        write!(f, "{s}")
    }
}

impl ConfigField for DFParquetWriterVersion {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        v.some(key, self, description)
    }

    fn set(&mut self, _: &str, value: &str) -> Result<()> {
        *self = DFParquetWriterVersion::from_str(value)?;
        Ok(())
    }
}

/// Convert `DFParquetWriterVersion` to parquet crate's `WriterVersion`
///
/// This conversion is infallible since `DFParquetWriterVersion` only contains
/// valid values that have been validated at configuration time.
#[cfg(feature = "parquet")]
impl From<DFParquetWriterVersion> for parquet::file::properties::WriterVersion {
    fn from(value: DFParquetWriterVersion) -> Self {
        match value {
            DFParquetWriterVersion::V1_0 => {
                parquet::file::properties::WriterVersion::PARQUET_1_0
            }
            DFParquetWriterVersion::V2_0 => {
                parquet::file::properties::WriterVersion::PARQUET_2_0
            }
        }
    }
}

/// Convert parquet crate's `WriterVersion` to `DFParquetWriterVersion`
///
/// This is used when converting from existing parquet writer properties,
/// such as when reading from proto or test code.
#[cfg(feature = "parquet")]
impl From<parquet::file::properties::WriterVersion> for DFParquetWriterVersion {
    fn from(version: parquet::file::properties::WriterVersion) -> Self {
        match version {
            parquet::file::properties::WriterVersion::PARQUET_1_0 => {
                DFParquetWriterVersion::V1_0
            }
            parquet::file::properties::WriterVersion::PARQUET_2_0 => {
                DFParquetWriterVersion::V2_0
            }
        }
    }
}

/// Parquet statistics levels supported by the writer
///
/// This enum validates statistics settings at configuration time, ensuring only
/// `none`, `chunk`, or `page` can be set via `SET` commands or deserialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DFParquetStatistics {
    /// Do not write statistics
    None,
    /// Write chunk-level statistics
    Chunk,
    /// Write page-level statistics
    Page,
}

impl FromStr for DFParquetStatistics {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "none" => Ok(Self::None),
            "chunk" => Ok(Self::Chunk),
            "page" => Ok(Self::Page),
            other => Err(DataFusionError::Configuration(format!(
                "Invalid parquet statistics setting: {other}. Expected one of: none, chunk, page"
            ))),
        }
    }
}

impl Display for DFParquetStatistics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::None => "none",
            Self::Chunk => "chunk",
            Self::Page => "page",
        };
        f.write_str(s)
    }
}

impl ConfigField for DFParquetStatistics {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        v.some(key, self, description)
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        if !key.is_empty() {
            return crate::error::_config_err!(
                "Config field parquet.statistics_enabled is a scalar DFParquetStatistics and does not have nested field \"{}\"",
                key
            );
        }

        *self = Self::from_str(value)?;
        Ok(())
    }
}

/// `ConfigField` for `Option<DFParquetStatistics>` parses before assigning so
/// an invalid value does not turn an unset option into the default.
impl ConfigField for Option<DFParquetStatistics> {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        match self {
            Some(statistics) => statistics.visit(v, key, description),
            None => v.none(key, description),
        }
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        if !key.is_empty() {
            return crate::error::_config_err!(
                "Config field parquet.statistics_enabled is a scalar Option<DFParquetStatistics> and does not have nested field \"{}\"",
                key
            );
        }

        *self = Some(DFParquetStatistics::from_str(value)?);
        Ok(())
    }

    fn reset(&mut self, key: &str) -> Result<()> {
        if key.is_empty() {
            *self = None;
            Ok(())
        } else {
            crate::error::_config_err!(
                "Config field parquet.statistics_enabled is a scalar Option<DFParquetStatistics> and does not have nested field \"{}\"",
                key
            )
        }
    }
}

#[cfg(feature = "parquet")]
impl From<DFParquetStatistics> for parquet::file::properties::EnabledStatistics {
    fn from(value: DFParquetStatistics) -> Self {
        match value {
            DFParquetStatistics::None => Self::None,
            DFParquetStatistics::Chunk => Self::Chunk,
            DFParquetStatistics::Page => Self::Page,
        }
    }
}

#[cfg(feature = "parquet")]
impl From<parquet::file::properties::EnabledStatistics> for DFParquetStatistics {
    fn from(value: parquet::file::properties::EnabledStatistics) -> Self {
        match value {
            parquet::file::properties::EnabledStatistics::None => Self::None,
            parquet::file::properties::EnabledStatistics::Chunk => Self::Chunk,
            parquet::file::properties::EnabledStatistics::Page => Self::Page,
        }
    }
}

/// Parquet compression codec, with a level for the codecs that take one
///
/// This enum validates compression settings at configuration time. An unknown
/// codec, a level on a codec that does not take one, a missing level on a codec
/// that requires one, or a level out of range is rejected by `SET` and by
/// deserialization instead of when the first file is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DFParquetCompression {
    /// No compression
    Uncompressed,
    /// Snappy
    Snappy,
    /// Gzip with a level from [`GZIP_LEVELS`]
    Gzip(u32),
    /// Brotli with a level from [`BROTLI_LEVELS`]
    Brotli(u32),
    /// LZ4 (Hadoop framed)
    Lz4,
    /// Zstd with a level from [`ZSTD_LEVELS`]
    Zstd(u32),
    /// LZ4 raw block format
    Lz4Raw,
}

/// Valid gzip levels, the same range as `parquet::basic::GzipLevel`
pub const GZIP_LEVELS: std::ops::RangeInclusive<u32> = 0..=9;
/// Valid brotli levels, the same range as `parquet::basic::BrotliLevel`
pub const BROTLI_LEVELS: std::ops::RangeInclusive<u32> = 0..=11;
/// Valid zstd levels. `parquet::basic::ZstdLevel` also accepts negative levels,
/// which the string form of this setting has never allowed.
pub const ZSTD_LEVELS: std::ops::RangeInclusive<u32> = 0..=22;

impl DFParquetCompression {
    /// Splits `gzip(2)` into `("gzip", Some(2))` and `snappy` into `("snappy", None)`
    fn split(setting: &str) -> Result<(&str, Option<u32>)> {
        let Some((codec, rest)) = setting.split_once('(') else {
            return Ok((setting, None));
        };
        let level = rest
            .strip_suffix(')')
            .and_then(|level| level.parse::<u32>().ok())
            .ok_or_else(|| {
                DataFusionError::Configuration(format!(
                    "Could not parse compression string. \
                    Got codec: {codec} and unknown level from {setting}"
                ))
            })?;
        Ok((codec, Some(level)))
    }

    fn without_level(codec: &str, level: Option<u32>) -> Result<()> {
        if level.is_some() {
            return Err(DataFusionError::Configuration(format!(
                "Compression {codec} does not support specifying a level"
            )));
        }
        Ok(())
    }

    fn level_in(
        codec: &str,
        level: Option<u32>,
        valid: std::ops::RangeInclusive<u32>,
    ) -> Result<u32> {
        let level = level.ok_or_else(|| {
            DataFusionError::Configuration(format!(
                "{codec} compression requires specifying a level such as {codec}(4)"
            ))
        })?;
        if !valid.contains(&level) {
            return Err(DataFusionError::Configuration(format!(
                "Invalid compression level {level} for {codec}. \
                Expected a level between {} and {}",
                valid.start(),
                valid.end()
            )));
        }
        Ok(level)
    }
}

impl FromStr for DFParquetCompression {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // ignore string literal chars passed from sqlparser i.e. remove single quotes
        let setting = s.replace('\'', "").to_lowercase();
        let (codec, level) = Self::split(&setting)?;
        match codec {
            "uncompressed" => {
                Self::without_level(codec, level)?;
                Ok(Self::Uncompressed)
            }
            "snappy" => {
                Self::without_level(codec, level)?;
                Ok(Self::Snappy)
            }
            "gzip" => Ok(Self::Gzip(Self::level_in(codec, level, GZIP_LEVELS)?)),
            "brotli" => Ok(Self::Brotli(Self::level_in(codec, level, BROTLI_LEVELS)?)),
            "lz4" => {
                Self::without_level(codec, level)?;
                Ok(Self::Lz4)
            }
            "zstd" => Ok(Self::Zstd(Self::level_in(codec, level, ZSTD_LEVELS)?)),
            "lz4_raw" => {
                Self::without_level(codec, level)?;
                Ok(Self::Lz4Raw)
            }
            _ => Err(DataFusionError::Configuration(format!(
                "Unknown or unsupported parquet compression: \
                {s}. Valid values are: uncompressed, snappy, gzip(level), \
                brotli(level), lz4, zstd(level), and lz4_raw."
            ))),
        }
    }
}

impl Display for DFParquetCompression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Uncompressed => f.write_str("uncompressed"),
            Self::Snappy => f.write_str("snappy"),
            Self::Gzip(level) => write!(f, "gzip({level})"),
            Self::Brotli(level) => write!(f, "brotli({level})"),
            Self::Lz4 => f.write_str("lz4"),
            Self::Zstd(level) => write!(f, "zstd({level})"),
            Self::Lz4Raw => f.write_str("lz4_raw"),
        }
    }
}

impl ConfigField for DFParquetCompression {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        v.some(key, self, description)
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        if !key.is_empty() {
            return crate::error::_config_err!(
                "Config field parquet.compression is a scalar DFParquetCompression and does not have nested field \"{}\"",
                key
            );
        }

        *self = Self::from_str(value)?;
        Ok(())
    }
}

/// `ConfigField` for `Option<DFParquetCompression>` parses before assigning so
/// an invalid value does not turn an unset option into the default.
impl ConfigField for Option<DFParquetCompression> {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        match self {
            Some(compression) => compression.visit(v, key, description),
            None => v.none(key, description),
        }
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        if !key.is_empty() {
            return crate::error::_config_err!(
                "Config field parquet.compression is a scalar Option<DFParquetCompression> and does not have nested field \"{}\"",
                key
            );
        }

        *self = Some(DFParquetCompression::from_str(value)?);
        Ok(())
    }

    fn reset(&mut self, key: &str) -> Result<()> {
        if key.is_empty() {
            *self = None;
            Ok(())
        } else {
            crate::error::_config_err!(
                "Config field parquet.compression is a scalar Option<DFParquetCompression> and does not have nested field \"{}\"",
                key
            )
        }
    }
}

#[cfg(feature = "parquet")]
impl From<DFParquetCompression> for parquet::basic::Compression {
    fn from(value: DFParquetCompression) -> Self {
        use parquet::basic::{BrotliLevel, GzipLevel, ZstdLevel};
        // The levels were checked against the same ranges when the value was parsed.
        match value {
            DFParquetCompression::Uncompressed => Self::UNCOMPRESSED,
            DFParquetCompression::Snappy => Self::SNAPPY,
            DFParquetCompression::Gzip(level) => {
                Self::GZIP(GzipLevel::try_new(level).expect("gzip level was validated"))
            }
            DFParquetCompression::Brotli(level) => Self::BROTLI(
                BrotliLevel::try_new(level).expect("brotli level was validated"),
            ),
            DFParquetCompression::Lz4 => Self::LZ4,
            DFParquetCompression::Zstd(level) => Self::ZSTD(
                ZstdLevel::try_new(level as i32).expect("zstd level was validated"),
            ),
            DFParquetCompression::Lz4Raw => Self::LZ4_RAW,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parquet_compression_parses_and_displays() {
        for (input, expected, displayed) in [
            (
                "uncompressed",
                DFParquetCompression::Uncompressed,
                "uncompressed",
            ),
            ("SNAPPY", DFParquetCompression::Snappy, "snappy"),
            ("gzip(6)", DFParquetCompression::Gzip(6), "gzip(6)"),
            ("Brotli(11)", DFParquetCompression::Brotli(11), "brotli(11)"),
            ("lz4", DFParquetCompression::Lz4, "lz4"),
            ("'zstd(3)'", DFParquetCompression::Zstd(3), "zstd(3)"),
            ("lz4_raw", DFParquetCompression::Lz4Raw, "lz4_raw"),
            ("zstd(0)", DFParquetCompression::Zstd(0), "zstd(0)"),
        ] {
            let parsed: DFParquetCompression = input.parse().unwrap();
            assert_eq!(parsed, expected, "input: {input}");
            assert_eq!(parsed.to_string(), displayed, "input: {input}");
            assert_eq!(displayed.parse::<DFParquetCompression>().unwrap(), parsed);
        }
    }

    #[test]
    fn parquet_compression_rejects_invalid_settings() {
        for (input, message) in [
            (
                "zstdd(3)",
                "Unknown or unsupported parquet compression: zstdd(3)",
            ),
            (
                "zstd",
                "zstd compression requires specifying a level such as zstd(4)",
            ),
            (
                "snappy(2)",
                "Compression snappy does not support specifying a level",
            ),
            ("zstd(x)", "Could not parse compression string"),
            ("zstd(3", "Could not parse compression string"),
            (
                "zstd(23)",
                "Invalid compression level 23 for zstd. Expected a level between 0 and 22",
            ),
            (
                "gzip(10)",
                "Invalid compression level 10 for gzip. Expected a level between 0 and 9",
            ),
            ("brotli(12)", "Invalid compression level 12 for brotli"),
            ("", "Unknown or unsupported parquet compression: "),
        ] {
            let err = input
                .parse::<DFParquetCompression>()
                .unwrap_err()
                .to_string();
            assert!(err.contains(message), "input {input:?}: {err}");
        }
    }

    /// The level ranges above must stay equal to the ones parquet enforces, or
    /// the infallible conversion would panic on a level parquet rejects.
    #[cfg(feature = "parquet")]
    #[test]
    fn parquet_compression_level_ranges_match_parquet() {
        use parquet::basic::{BrotliLevel, GzipLevel, ZstdLevel};

        for level in GZIP_LEVELS {
            assert!(GzipLevel::try_new(level).is_ok(), "gzip({level})");
        }
        assert!(GzipLevel::try_new(GZIP_LEVELS.end() + 1).is_err());

        for level in BROTLI_LEVELS {
            assert!(BrotliLevel::try_new(level).is_ok(), "brotli({level})");
        }
        assert!(BrotliLevel::try_new(BROTLI_LEVELS.end() + 1).is_err());

        for level in ZSTD_LEVELS {
            assert!(ZstdLevel::try_new(level as i32).is_ok(), "zstd({level})");
        }
        assert!(ZstdLevel::try_new(*ZSTD_LEVELS.end() as i32 + 1).is_err());
    }
}
