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

use std::fmt::Display;
use std::str::FromStr;

use crate::DataFusionError;

/// Readable file compression type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompressionTypeVariant {
    /// Gzip-ed file
    GZIP,
    /// Bzip2-ed file
    BZIP2,
    /// Xz-ed file (liblzma)
    XZ,
    /// Zstd-ed file,
    ZSTD,
    /// Uncompressed file
    UNCOMPRESSED,
}

impl FromStr for CompressionTypeVariant {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_uppercase();
        match s.as_str() {
            "GZIP" | "GZ" => Ok(Self::GZIP),
            "BZIP2" | "BZ2" => Ok(Self::BZIP2),
            "XZ" => Ok(Self::XZ),
            "ZST" | "ZSTD" => Ok(Self::ZSTD),
            "" | "UNCOMPRESSED" => Ok(Self::UNCOMPRESSED),
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported file compression type {s}"
            ))),
        }
    }
}

impl Display for CompressionTypeVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            Self::GZIP => "GZIP",
            Self::BZIP2 => "BZIP2",
            Self::XZ => "XZ",
            Self::ZSTD => "ZSTD",
            Self::UNCOMPRESSED => "",
        };
        write!(f, "{str}")
    }
}

impl CompressionTypeVariant {
    pub const fn is_compressed(&self) -> bool {
        !matches!(self, &Self::UNCOMPRESSED)
    }
}

/// CSV quote style
///
/// Controls when fields are quoted when writing CSV files.
/// Corresponds to [`arrow::csv::QuoteStyle`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum CsvQuoteStyle {
    /// Quote all fields
    Always,
    /// Only quote fields when necessary (default)
    #[default]
    Necessary,
    /// Quote all non-numeric fields
    NonNumeric,
    /// Never quote fields
    Never,
}

impl FromStr for CsvQuoteStyle {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "always" => Ok(Self::Always),
            "necessary" => Ok(Self::Necessary),
            "non_numeric" | "nonnumeric" => Ok(Self::NonNumeric),
            "never" => Ok(Self::Never),
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported CSV quote style {s}"
            ))),
        }
    }
}

impl From<CsvQuoteStyle> for arrow::csv::QuoteStyle {
    fn from(style: CsvQuoteStyle) -> Self {
        match style {
            CsvQuoteStyle::Always => Self::Always,
            CsvQuoteStyle::NonNumeric => Self::NonNumeric,
            CsvQuoteStyle::Never => Self::Never,
            CsvQuoteStyle::Necessary => Self::Necessary,
        }
    }
}

impl Display for CsvQuoteStyle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            Self::Always => "Always",
            Self::Necessary => "Necessary",
            Self::NonNumeric => "NonNumeric",
            Self::Never => "Never",
        };
        write!(f, "{str}")
    }
}
