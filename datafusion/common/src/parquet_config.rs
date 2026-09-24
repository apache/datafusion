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

/// Time unit for reading Parquet INT96 timestamps
///
/// This enum validates `coerce_int96` at configuration time, so only `ns`,
/// `us`, `ms` or `s` can be set via `SET` commands or deserialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DFTimeUnit {
    Nanosecond,
    Microsecond,
    Millisecond,
    Second,
}

impl FromStr for DFTimeUnit {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "ns" => Ok(Self::Nanosecond),
            "us" => Ok(Self::Microsecond),
            "ms" => Ok(Self::Millisecond),
            "s" => Ok(Self::Second),
            other => Err(DataFusionError::Configuration(format!(
                "Invalid parquet coerce_int96 setting: {other}. Expected one of: ns, us, ms, s"
            ))),
        }
    }
}

impl Display for DFTimeUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Nanosecond => "ns",
            Self::Microsecond => "us",
            Self::Millisecond => "ms",
            Self::Second => "s",
        };
        f.write_str(s)
    }
}

impl ConfigField for DFTimeUnit {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        v.some(key, self, description)
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        if !key.is_empty() {
            return crate::error::_config_err!(
                "Config field parquet.coerce_int96 is a scalar DFTimeUnit and does not have nested field \"{}\"",
                key
            );
        }

        *self = Self::from_str(value)?;
        Ok(())
    }
}

/// `ConfigField` for `Option<DFTimeUnit>` parses before assigning so an
/// invalid value does not turn an unset option into the default.
impl ConfigField for Option<DFTimeUnit> {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        match self {
            Some(time_unit) => time_unit.visit(v, key, description),
            None => v.none(key, description),
        }
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        if !key.is_empty() {
            return crate::error::_config_err!(
                "Config field parquet.coerce_int96 is a scalar Option<DFTimeUnit> and does not have nested field \"{}\"",
                key
            );
        }

        *self = Some(DFTimeUnit::from_str(value)?);
        Ok(())
    }

    fn reset(&mut self, key: &str) -> Result<()> {
        if key.is_empty() {
            *self = None;
            Ok(())
        } else {
            crate::error::_config_err!(
                "Config field parquet.coerce_int96 is a scalar Option<DFTimeUnit> and does not have nested field \"{}\"",
                key
            )
        }
    }
}

impl From<DFTimeUnit> for arrow::datatypes::TimeUnit {
    fn from(value: DFTimeUnit) -> Self {
        match value {
            DFTimeUnit::Nanosecond => Self::Nanosecond,
            DFTimeUnit::Microsecond => Self::Microsecond,
            DFTimeUnit::Millisecond => Self::Millisecond,
            DFTimeUnit::Second => Self::Second,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_unit_parses_and_displays() {
        for (input, expected, displayed) in [
            ("ns", DFTimeUnit::Nanosecond, "ns"),
            ("US", DFTimeUnit::Microsecond, "us"),
            ("ms", DFTimeUnit::Millisecond, "ms"),
            ("s", DFTimeUnit::Second, "s"),
        ] {
            let parsed: DFTimeUnit = input.parse().unwrap();
            assert_eq!(parsed, expected, "input: {input}");
            assert_eq!(parsed.to_string(), displayed);
        }
        for input in ["nanos", "", "ns "] {
            let err = input.parse::<DFTimeUnit>().unwrap_err().to_string();
            assert!(
                err.contains("Invalid parquet coerce_int96 setting"),
                "input {input:?}: {err}"
            );
        }
    }
}
