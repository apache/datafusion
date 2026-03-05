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

/// Metadata key for storing Parquet field IDs in Arrow field metadata.
///
/// Field IDs are stable identifiers for columns in Parquet files that enable
/// schema evolution with renamed or reordered columns. When `field_id_enabled`
/// is true, DataFusion stores field IDs from Parquet files in Arrow field metadata
/// using this key, allowing columns to be matched by ID instead of name.
///
/// # Example
/// ```rust
/// use datafusion_common::parquet_config::PARQUET_FIELD_ID_META_KEY;
/// use arrow::datatypes::Field;
/// use std::collections::HashMap;
///
/// let mut metadata = HashMap::new();
/// metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), "42".to_string());
/// let field = Field::new("my_column", arrow::datatypes::DataType::Int32, false)
///     .with_metadata(metadata);
///
/// // Later, retrieve the field ID
/// let field_id = field.metadata()
///     .get(PARQUET_FIELD_ID_META_KEY)
///     .and_then(|s| s.parse::<i32>().ok());
/// assert_eq!(field_id, Some(42));
/// ```
pub const PARQUET_FIELD_ID_META_KEY: &str = "PARQUET:field_id";

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
