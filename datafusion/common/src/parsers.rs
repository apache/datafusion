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

use std::result;
use std::str::FromStr;

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
    type Err = ParserError;

    fn from_str(s: &str) -> result::Result<Self, ParserError> {
        let s = s.to_uppercase();
        match s.as_str() {
            "GZIP" | "GZ" => Ok(Self::GZIP),
            "BZIP2" | "BZ2" => Ok(Self::BZIP2),
            "XZ" => Ok(Self::XZ),
            "ZST" | "ZSTD" => Ok(Self::ZSTD),
            "" | "UNCOMPRESSED" => Ok(Self::UNCOMPRESSED),
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
            Self::ZSTD => "ZSTD",
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
