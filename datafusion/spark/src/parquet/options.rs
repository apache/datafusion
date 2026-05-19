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

//! Options controlling Spark-compatible Parquet reading.
//!
//! See [`SparkParquetOptions`] for the full list of settings.

/// Spark expression evaluation mode.
///
/// Spark supports three evaluation modes when evaluating expressions, which affect
/// the behavior when processing input values that are invalid or would result in an
/// error, such as divide-by-zero errors, and also affect behavior when converting
/// between types.
#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub enum EvalMode {
    /// Default behaviour in Spark prior to Spark 4.0. Silently ignores or replaces
    /// errors during SQL operations and enables implicit type conversions.
    Legacy,
    /// Adheres to the ANSI SQL standard for error handling. Throws exceptions for
    /// operations that would otherwise silently succeed and does not perform
    /// implicit type conversions.
    Ansi,
    /// Same as [`EvalMode::Ansi`] except that errors become NULL values rather than
    /// failing the entire query.
    Try,
}

/// Options controlling Spark-compatible Parquet type conversion.
///
/// Many of these mirror the equivalent Spark configuration entries:
///
/// * [`use_field_id`](Self::use_field_id) /
///   [`ignore_missing_field_id`](Self::ignore_missing_field_id) mirror
///   `spark.sql.parquet.fieldId.read.enabled` and
///   `spark.sql.parquet.fieldId.read.ignoreMissing`.
/// * [`return_null_struct_if_all_fields_missing`](Self::return_null_struct_if_all_fields_missing)
///   mirrors `spark.sql.legacy.parquet.returnNullStructIfAllFieldsMissing`
///   (SPARK-53535, Spark 4.1+).
/// * [`allow_type_promotion`](Self::allow_type_promotion) tracks Spark's
///   per-version vectorized-reader policy for widening reads (e.g. INT32 → INT64
///   was rejected in Spark 3.x but is allowed in Spark 4.x).
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct SparkParquetOptions {
    /// Spark evaluation mode (forwarded to nested-type conversion).
    pub eval_mode: EvalMode,
    /// Session timezone, used when casting to/from timezone-related types.
    // TODO: replace with `Tz` to avoid repeated parsing.
    pub timezone: String,
    /// Allow casts that are supported but not guaranteed to be 100% compatible
    /// with Spark.
    pub allow_incompat: bool,
    /// Allow casting unsigned integers to signed integers (used by the
    /// Parquet schema adapter for Iceberg / Arrow files that use unsigned types).
    pub allow_cast_unsigned_ints: bool,
    /// Whether to always represent decimals using 128 bits. If `false`, the
    /// native reader may represent decimals using 32 or 64 bits depending on
    /// the precision.
    pub use_decimal_128: bool,
    /// Whether to read dates/timestamps written in the legacy hybrid Julian +
    /// Gregorian calendar without rebasing. If `false`, throw exceptions
    /// instead. If the Spark type is `TimestampNTZ`, this should be `true`.
    pub use_legacy_date_timestamp_or_ntz: bool,
    /// Whether schema field names are matched case-sensitively.
    pub case_sensitive: bool,
    /// SPARK-53535 (Spark 4.1+): when reading a struct whose requested fields
    /// are all missing in the Parquet file, `true` returns the entire struct
    /// as null (pre-4.1 legacy behavior); `false` preserves the parent struct's
    /// nullness from the file so non-null parents return a struct of all-null
    /// fields.
    pub return_null_struct_if_all_fields_missing: bool,
    /// When `true`, resolve fields by `parquet.field.id` metadata instead of by
    /// name (mirrors `spark.sql.parquet.fieldId.read.enabled`). Only takes
    /// effect when both physical and logical fields actually carry IDs.
    pub use_field_id: bool,
    /// When `false` (Spark's default), reading a file that has no field ids
    /// while the requested schema does carry ids raises a runtime error rather
    /// than silently producing nulls (mirrors
    /// `spark.sql.parquet.fieldId.read.ignoreMissing`).
    pub ignore_missing_field_id: bool,
    /// Whether type promotion (schema evolution) is allowed, e.g. INT32 ->
    /// INT64, FLOAT -> DOUBLE. Spark 3.x rejects these in the vectorized
    /// reader; Spark 4.x allows them.
    pub allow_type_promotion: bool,
}

impl SparkParquetOptions {
    /// Create a new [`SparkParquetOptions`] with the given evaluation mode,
    /// timezone, and `allow_incompat` flag. All other fields default to values
    /// matching Spark 3.x's vectorized-reader behavior.
    pub fn new(eval_mode: EvalMode, timezone: &str, allow_incompat: bool) -> Self {
        Self {
            eval_mode,
            timezone: timezone.to_string(),
            allow_incompat,
            allow_cast_unsigned_ints: false,
            use_decimal_128: false,
            use_legacy_date_timestamp_or_ntz: false,
            case_sensitive: false,
            return_null_struct_if_all_fields_missing: true,
            use_field_id: false,
            ignore_missing_field_id: false,
            allow_type_promotion: false,
        }
    }

    /// Create a new [`SparkParquetOptions`] without a timezone. Useful for
    /// conversions that do not involve timezone-aware types.
    pub fn new_without_timezone(eval_mode: EvalMode, allow_incompat: bool) -> Self {
        Self::new(eval_mode, "", allow_incompat)
    }
}
