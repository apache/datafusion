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

//! Errors raised by the Spark-compatible Parquet schema adapter.
//!
//! These mirror the small subset of [Spark's error taxonomy] that the
//! vectorized Parquet reader can raise. They are surfaced through
//! [`datafusion_common::DataFusionError::External`] so callers that bridge
//! to a Spark JVM (e.g. Comet) can pattern-match on the inner type and
//! convert to the matching Spark exception class.
//!
//! [Spark's error taxonomy]: https://spark.apache.org/docs/latest/sql-error-conditions.html

use std::error::Error;
use std::fmt;

use datafusion_common::DataFusionError;

/// Errors raised by the Spark-compatible Parquet schema adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParquetSchemaError {
    /// Mirrors Spark's `_LEGACY_ERROR_TEMP_2093`: when reading in
    /// case-insensitive mode, more than one Parquet field matches the
    /// requested name.
    DuplicateFieldCaseInsensitive {
        required_field_name: String,
        matched_fields: String,
    },

    /// Mirrors Spark's `_LEGACY_ERROR_TEMP_2094`: multiple Parquet fields
    /// share the same field id when the read schema requested an id-based
    /// lookup.
    DuplicateFieldByFieldId {
        required_id: i32,
        matched_fields: String,
    },

    /// Mirrors the runtime error raised in Spark's `ParquetReadSupport`
    /// when the Spark read schema requests Parquet field-id matching but the
    /// file carries no field ids and `spark.sql.parquet.fieldId.read.ignoreMissing`
    /// is `false`.
    MissingFieldIds,

    /// Schema mismatch when reading a Parquet column under a requested schema
    /// that's incompatible with the physical column type. Mirrors Spark's
    /// `SchemaColumnConvertNotSupportedException`. The `file_path` may be
    /// empty when the rejection happens at planning time (the schema adapter
    /// does not carry the file path).
    SchemaConvert {
        file_path: String,
        column: String,
        physical_type: String,
        spark_type: String,
    },
}

impl fmt::Display for ParquetSchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateFieldCaseInsensitive {
                required_field_name,
                matched_fields,
            } => write!(
                f,
                "[_LEGACY_ERROR_TEMP_2093] Found duplicate field(s) \"{required_field_name}\": [{matched_fields}] in case-insensitive mode"
            ),
            Self::DuplicateFieldByFieldId {
                required_id,
                matched_fields,
            } => write!(
                f,
                "[_LEGACY_ERROR_TEMP_2094] Found duplicate field(s) by id: id={required_id} matches [{matched_fields}] in id-lookup mode"
            ),
            Self::MissingFieldIds => write!(
                f,
                "Spark read schema expects field Ids, but Parquet file schema doesn't contain any field Ids. \
                 Please remove the field ids from Spark schema or ignore missing ids by setting \
                 `spark.sql.parquet.fieldId.read.ignoreMissing = true`"
            ),
            Self::SchemaConvert {
                file_path,
                column,
                physical_type,
                spark_type,
            } => write!(
                f,
                "Parquet column cannot be converted in file {file_path}. \
                 Column: [{column}], Expected: {spark_type}, Found: {physical_type}"
            ),
        }
    }
}

impl Error for ParquetSchemaError {}

impl From<ParquetSchemaError> for DataFusionError {
    fn from(value: ParquetSchemaError) -> Self {
        DataFusionError::External(Box::new(value))
    }
}
