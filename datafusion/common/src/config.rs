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

//! Runtime configuration, via [`ConfigOptions`]

use arrow_ipc::CompressionType;

#[cfg(feature = "parquet_encryption")]
use crate::encryption::{FileDecryptionProperties, FileEncryptionProperties};
use crate::error::_config_err;
use crate::format::{ExplainAnalyzeLevel, ExplainFormat};
use crate::parsers::CompressionTypeVariant;
use crate::utils::get_available_parallelism;
use crate::{DataFusionError, Result};
#[cfg(feature = "parquet_encryption")]
use hex;
use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fmt::{self, Display};
use std::str::FromStr;
#[cfg(feature = "parquet_encryption")]
use std::sync::Arc;

/// A macro that wraps a configuration struct and automatically derives
/// [`Default`] and [`ConfigField`] for it, allowing it to be used
/// in the [`ConfigOptions`] configuration tree.
///
/// `transform` is used to normalize values before parsing.
///
/// For example,
///
/// ```ignore
/// config_namespace! {
///    /// Amazing config
///    pub struct MyConfig {
///        /// Field 1 doc
///        field1: String, transform = str::to_lowercase, default = "".to_string()
///
///        /// Field 2 doc
///        field2: usize, default = 232
///
///        /// Field 3 doc
///        field3: Option<usize>, default = None
///    }
/// }
/// ```
///
/// Will generate
///
/// ```ignore
/// /// Amazing config
/// #[derive(Debug, Clone)]
/// #[non_exhaustive]
/// pub struct MyConfig {
///     /// Field 1 doc
///     field1: String,
///     /// Field 2 doc
///     field2: usize,
///     /// Field 3 doc
///     field3: Option<usize>,
/// }
/// impl ConfigField for MyConfig {
///     fn set(&mut self, key: &str, value: &str) -> Result<()> {
///         let (key, rem) = key.split_once('.').unwrap_or((key, ""));
///         match key {
///             "field1" => {
///                 let value = str::to_lowercase(value);
///                 self.field1.set(rem, value.as_ref())
///             },
///             "field2" => self.field2.set(rem, value.as_ref()),
///             "field3" => self.field3.set(rem, value.as_ref()),
///             _ => _internal_err!(
///                 "Config value \"{}\" not found on MyConfig",
///                 key
///             ),
///         }
///     }
///
///     fn visit<V: Visit>(&self, v: &mut V, key_prefix: &str, _description: &'static str) {
///         let key = format!("{}.field1", key_prefix);
///         let desc = "Field 1 doc";
///         self.field1.visit(v, key.as_str(), desc);
///         let key = format!("{}.field2", key_prefix);
///         let desc = "Field 2 doc";
///         self.field2.visit(v, key.as_str(), desc);
///         let key = format!("{}.field3", key_prefix);
///         let desc = "Field 3 doc";
///         self.field3.visit(v, key.as_str(), desc);
///     }
/// }
///
/// impl Default for MyConfig {
///     fn default() -> Self {
///         Self {
///             field1: "".to_string(),
///             field2: 232,
///             field3: None,
///         }
///     }
/// }
/// ```
///
/// NB: Misplaced commas may result in nonsensical errors
#[macro_export]
macro_rules! config_namespace {
    (
        $(#[doc = $struct_d:tt])* // Struct-level documentation attributes
        $(#[deprecated($($struct_depr:tt)*)])? // Optional struct-level deprecated attribute
        $(#[allow($($struct_de:tt)*)])?
        $vis:vis struct $struct_name:ident {
            $(
                $(#[doc = $d:tt])* // Field-level documentation attributes
                $(#[deprecated($($field_depr:tt)*)])? // Optional field-level deprecated attribute
                $(#[allow($($field_de:tt)*)])?
                $field_vis:vis $field_name:ident : $field_type:ty,
                $(warn = $warn:expr,)?
                $(transform = $transform:expr,)?
                default = $default:expr
            )*$(,)*
        }
    ) => {
        $(#[doc = $struct_d])* // Apply struct documentation
        $(#[deprecated($($struct_depr)*)])? // Apply struct deprecation
        $(#[allow($($struct_de)*)])?
        #[derive(Debug, Clone, PartialEq)]
        $vis struct $struct_name {
            $(
                $(#[doc = $d])* // Apply field documentation
                $(#[deprecated($($field_depr)*)])? // Apply field deprecation
                $(#[allow($($field_de)*)])?
                $field_vis $field_name: $field_type,
            )*
        }

        impl $crate::config::ConfigField for $struct_name {
            fn set(&mut self, key: &str, value: &str) -> $crate::error::Result<()> {
                let (key, rem) = key.split_once('.').unwrap_or((key, ""));
                match key {
                    $(
                        stringify!($field_name) => {
                            // Safely apply deprecated attribute if present
                            // $(#[allow(deprecated)])?
                            {
                                $(let value = $transform(value);)? // Apply transformation if specified
                                #[allow(deprecated)]
                                let ret = self.$field_name.set(rem, value.as_ref());

                                $(if !$warn.is_empty() {
                                    let default: $field_type = $default;
                                    #[allow(deprecated)]
                                    if default != self.$field_name {
                                        log::warn!($warn);
                                    }
                                })? // Log warning if specified, and the value is not the default
                                ret
                            }
                        },
                    )*
                    _ => return $crate::error::_config_err!(
                        "Config value \"{}\" not found on {}", key, stringify!($struct_name)
                    )
                }
            }

            fn visit<V: $crate::config::Visit>(&self, v: &mut V, key_prefix: &str, _description: &'static str) {
                $(
                    let key = format!(concat!("{}.", stringify!($field_name)), key_prefix);
                    let desc = concat!($($d),*).trim();
                    #[allow(deprecated)]
                    self.$field_name.visit(v, key.as_str(), desc);
                )*
            }

            fn reset(&mut self, key: &str) -> $crate::error::Result<()> {
                let (key, rem) = key.split_once('.').unwrap_or((key, ""));
                match key {
                    $(
                        stringify!($field_name) => {
                            #[allow(deprecated)]
                            {
                                if rem.is_empty() {
                                    let default_value: $field_type = $default;
                                    self.$field_name = default_value;
                                    Ok(())
                                } else {
                                    self.$field_name.reset(rem)
                                }
                            }
                        },
                    )*
                    _ => $crate::error::_config_err!(
                        "Config value \"{}\" not found on {}",
                        key,
                        stringify!($struct_name)
                    ),
                }
            }
        }
        impl Default for $struct_name {
            fn default() -> Self {
                #[allow(deprecated)]
                Self {
                    $($field_name: $default),*
                }
            }
        }
    }
}

config_namespace! {
    /// Options related to catalog and directory scanning
    ///
    /// See also: [`SessionConfig`]
    ///
    /// [`SessionConfig`]: https://docs.rs/datafusion/latest/datafusion/prelude/struct.SessionConfig.html
    pub struct CatalogOptions {
        /// Whether the default catalog and schema should be created automatically.
        pub create_default_catalog_and_schema: bool, default = true

        /// The default catalog name - this impacts what SQL queries use if not specified
        pub default_catalog: String, default = "datafusion".to_string()

        /// The default schema name - this impacts what SQL queries use if not specified
        pub default_schema: String, default = "public".to_string()

        /// Should DataFusion provide access to `information_schema`
        /// virtual tables for displaying schema information
        pub information_schema: bool, default = false

        /// Location scanned to load tables for `default` schema
        pub location: Option<String>, default = None

        /// Type of `TableProvider` to use when loading `default` schema
        pub format: Option<String>, default = None

        /// Default value for `format.has_header` for `CREATE EXTERNAL TABLE`
        /// if not specified explicitly in the statement.
        pub has_header: bool, default = true

        /// Specifies whether newlines in (quoted) CSV values are supported.
        ///
        /// This is the default value for `format.newlines_in_values` for `CREATE EXTERNAL TABLE`
        /// if not specified explicitly in the statement.
        ///
        /// Parsing newlines in quoted values may be affected by execution behaviour such as
        /// parallel file scanning. Setting this to `true` ensures that newlines in values are
        /// parsed successfully, which may reduce performance.
        pub newlines_in_values: bool, default = false
    }
}

config_namespace! {
    /// Options related to SQL parser
    ///
    /// See also: [`SessionConfig`]
    ///
    /// [`SessionConfig`]: https://docs.rs/datafusion/latest/datafusion/prelude/struct.SessionConfig.html
    pub struct SqlParserOptions {
        /// When set to true, SQL parser will parse float as decimal type
        pub parse_float_as_decimal: bool, default = false

        /// When set to true, SQL parser will normalize ident (convert ident to lowercase when not quoted)
        pub enable_ident_normalization: bool, default = true

        /// When set to true, SQL parser will normalize options value (convert value to lowercase).
        /// Note that this option is ignored and will be removed in the future. All case-insensitive values
        /// are normalized automatically.
        pub enable_options_value_normalization: bool, warn = "`enable_options_value_normalization` is deprecated and ignored", default = false

        /// Configure the SQL dialect used by DataFusion's parser; supported values include: Generic,
        /// MySQL, PostgreSQL, Hive, SQLite, Snowflake, Redshift, MsSQL, ClickHouse, BigQuery, Ansi, DuckDB and Databricks.
        pub dialect: Dialect, default = Dialect::Generic
        // no need to lowercase because `sqlparser::dialect_from_str`] is case-insensitive

        /// If true, permit lengths for `VARCHAR` such as `VARCHAR(20)`, but
        /// ignore the length. If false, error if a `VARCHAR` with a length is
        /// specified. The Arrow type system does not have a notion of maximum
        /// string length and thus DataFusion can not enforce such limits.
        pub support_varchar_with_length: bool, default = true

        /// If true, string types (VARCHAR, CHAR, Text, and String) are mapped to `Utf8View` during SQL planning.
        /// If false, they are mapped to `Utf8`.
        /// Default is true.
        pub map_string_types_to_utf8view: bool, default = true

        /// When set to true, the source locations relative to the original SQL
        /// query (i.e. [`Span`](https://docs.rs/sqlparser/latest/sqlparser/tokenizer/struct.Span.html)) will be collected
        /// and recorded in the logical plan nodes.
        pub collect_spans: bool, default = false

        /// Specifies the recursion depth limit when parsing complex SQL Queries
        pub recursion_limit: usize, default = 50

        /// Specifies the default null ordering for query results. There are 4 options:
        /// - `nulls_max`: Nulls appear last in ascending order.
        /// - `nulls_min`: Nulls appear first in ascending order.
        /// - `nulls_first`: Nulls always be first in any order.
        /// - `nulls_last`: Nulls always be last in any order.
        ///
        /// By default, `nulls_max` is used to follow Postgres's behavior.
        /// postgres rule: <https://www.postgresql.org/docs/current/queries-order.html>
        pub default_null_ordering: String, default = "nulls_max".to_string()
    }
}

/// This is the SQL dialect used by DataFusion's parser.
/// This mirrors [sqlparser::dialect::Dialect](https://docs.rs/sqlparser/latest/sqlparser/dialect/trait.Dialect.html)
/// trait in order to offer an easier API and avoid adding the `sqlparser` dependency
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    #[default]
    Generic,
    MySQL,
    PostgreSQL,
    Hive,
    SQLite,
    Snowflake,
    Redshift,
    MsSQL,
    ClickHouse,
    BigQuery,
    Ansi,
    DuckDB,
    Databricks,
}

impl AsRef<str> for Dialect {
    fn as_ref(&self) -> &str {
        match self {
            Self::Generic => "generic",
            Self::MySQL => "mysql",
            Self::PostgreSQL => "postgresql",
            Self::Hive => "hive",
            Self::SQLite => "sqlite",
            Self::Snowflake => "snowflake",
            Self::Redshift => "redshift",
            Self::MsSQL => "mssql",
            Self::ClickHouse => "clickhouse",
            Self::BigQuery => "bigquery",
            Self::Ansi => "ansi",
            Self::DuckDB => "duckdb",
            Self::Databricks => "databricks",
        }
    }
}

impl FromStr for Dialect {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let value = match s.to_ascii_lowercase().as_str() {
            "generic" => Self::Generic,
            "mysql" => Self::MySQL,
            "postgresql" | "postgres" => Self::PostgreSQL,
            "hive" => Self::Hive,
            "sqlite" => Self::SQLite,
            "snowflake" => Self::Snowflake,
            "redshift" => Self::Redshift,
            "mssql" => Self::MsSQL,
            "clickhouse" => Self::ClickHouse,
            "bigquery" => Self::BigQuery,
            "ansi" => Self::Ansi,
            "duckdb" => Self::DuckDB,
            "databricks" => Self::Databricks,
            other => {
                let error_message = format!(
                    "Invalid Dialect: {other}. Expected one of: Generic, MySQL, PostgreSQL, Hive, SQLite, Snowflake, Redshift, MsSQL, ClickHouse, BigQuery, Ansi, DuckDB, Databricks"
                );
                return Err(DataFusionError::Configuration(error_message));
            }
        };
        Ok(value)
    }
}

impl ConfigField for Dialect {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        v.some(key, self, description)
    }

    fn set(&mut self, _: &str, value: &str) -> Result<()> {
        *self = Self::from_str(value)?;
        Ok(())
    }
}

impl Display for Dialect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let str = self.as_ref();
        write!(f, "{str}")
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum SpillCompression {
    Zstd,
    Lz4Frame,
    #[default]
    Uncompressed,
}

impl FromStr for SpillCompression {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "zstd" => Ok(Self::Zstd),
            "lz4_frame" => Ok(Self::Lz4Frame),
            "uncompressed" | "" => Ok(Self::Uncompressed),
            other => Err(DataFusionError::Configuration(format!(
                "Invalid Spill file compression type: {other}. Expected one of: zstd, lz4_frame, uncompressed"
            ))),
        }
    }
}

impl ConfigField for SpillCompression {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        v.some(key, self, description)
    }

    fn set(&mut self, _: &str, value: &str) -> Result<()> {
        *self = SpillCompression::from_str(value)?;
        Ok(())
    }
}

impl Display for SpillCompression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let str = match self {
            Self::Zstd => "zstd",
            Self::Lz4Frame => "lz4_frame",
            Self::Uncompressed => "uncompressed",
        };
        write!(f, "{str}")
    }
}

impl From<SpillCompression> for Option<CompressionType> {
    fn from(c: SpillCompression) -> Self {
        match c {
            SpillCompression::Zstd => Some(CompressionType::ZSTD),
            SpillCompression::Lz4Frame => Some(CompressionType::LZ4_FRAME),
            SpillCompression::Uncompressed => None,
        }
    }
}

config_namespace! {
    /// Options related to query execution
    ///
    /// See also: [`SessionConfig`]
    ///
    /// [`SessionConfig`]: https://docs.rs/datafusion/latest/datafusion/prelude/struct.SessionConfig.html
    pub struct ExecutionOptions {
        /// Default batch size while creating new batches, it's especially useful for
        /// buffer-in-memory batches since creating tiny batches would result in too much
        /// metadata memory consumption
        pub batch_size: usize, default = 8192

        /// When set to true, record batches will be examined between each operator and
        /// small batches will be coalesced into larger batches. This is helpful when there
        /// are highly selective filters or joins that could produce tiny output batches. The
        /// target batch size is determined by the configuration setting
        pub coalesce_batches: bool, default = true

        /// Should DataFusion collect statistics when first creating a table.
        /// Has no effect after the table is created. Applies to the default
        /// `ListingTableProvider` in DataFusion. Defaults to true.
        pub collect_statistics: bool, default = true

        /// Number of partitions for query execution. Increasing partitions can increase
        /// concurrency.
        ///
        /// Defaults to the number of CPU cores on the system
        pub target_partitions: usize, transform = ExecutionOptions::normalized_parallelism, default = get_available_parallelism()

        /// The default time zone
        ///
        /// Some functions, e.g. `now` return timestamps in this time zone
        pub time_zone: Option<String>, default = None

        /// Parquet options
        pub parquet: ParquetOptions, default = Default::default()

        /// Fan-out during initial physical planning.
        ///
        /// This is mostly use to plan `UNION` children in parallel.
        ///
        /// Defaults to the number of CPU cores on the system
        pub planning_concurrency: usize, transform = ExecutionOptions::normalized_parallelism, default = get_available_parallelism()

        /// When set to true, skips verifying that the schema produced by
        /// planning the input of `LogicalPlan::Aggregate` exactly matches the
        /// schema of the input plan.
        ///
        /// When set to false, if the schema does not match exactly
        /// (including nullability and metadata), a planning error will be raised.
        ///
        /// This is used to workaround bugs in the planner that are now caught by
        /// the new schema verification step.
        pub skip_physical_aggregate_schema_check: bool, default = false

        /// Sets the compression codec used when spilling data to disk.
        ///
        /// Since datafusion writes spill files using the Arrow IPC Stream format,
        /// only codecs supported by the Arrow IPC Stream Writer are allowed.
        /// Valid values are: uncompressed, lz4_frame, zstd.
        /// Note: lz4_frame offers faster (de)compression, but typically results in
        /// larger spill files. In contrast, zstd achieves
        /// higher compression ratios at the cost of slower (de)compression speed.
        pub spill_compression: SpillCompression, default = SpillCompression::Uncompressed

        /// Specifies the reserved memory for each spillable sort operation to
        /// facilitate an in-memory merge.
        ///
        /// When a sort operation spills to disk, the in-memory data must be
        /// sorted and merged before being written to a file. This setting reserves
        /// a specific amount of memory for that in-memory sort/merge process.
        ///
        /// Note: This setting is irrelevant if the sort operation cannot spill
        /// (i.e., if there's no `DiskManager` configured).
        pub sort_spill_reservation_bytes: usize, default = 10 * 1024 * 1024

        /// When sorting, below what size should data be concatenated
        /// and sorted in a single RecordBatch rather than sorted in
        /// batches and merged.
        pub sort_in_place_threshold_bytes: usize, default = 1024 * 1024

        /// Maximum size in bytes for individual spill files before rotating to a new file.
        ///
        /// When operators spill data to disk (e.g., RepartitionExec), they write
        /// multiple batches to the same file until this size limit is reached, then rotate
        /// to a new file. This reduces syscall overhead compared to one-file-per-batch
        /// while preventing files from growing too large.
        ///
        /// A larger value reduces file creation overhead but may hold more disk space.
        /// A smaller value creates more files but allows finer-grained space reclamation
        /// as files can be deleted once fully consumed.
        ///
        /// Now only `RepartitionExec` supports this spill file rotation feature, other spilling operators
        /// may create spill files larger than the limit.
        ///
        /// Default: 128 MB
        pub max_spill_file_size_bytes: usize, default = 128 * 1024 * 1024

        /// Number of files to read in parallel when inferring schema and statistics
        pub meta_fetch_concurrency: usize, default = 32

        /// Guarantees a minimum level of output files running in parallel.
        /// RecordBatches will be distributed in round robin fashion to each
        /// parallel writer. Each writer is closed and a new file opened once
        /// soft_max_rows_per_output_file is reached.
        pub minimum_parallel_output_files: usize, default = 4

        /// Target number of rows in output files when writing multiple.
        /// This is a soft max, so it can be exceeded slightly. There also
        /// will be one file smaller than the limit if the total
        /// number of rows written is not roughly divisible by the soft max
        pub soft_max_rows_per_output_file: usize, default = 50000000

        /// This is the maximum number of RecordBatches buffered
        /// for each output file being worked. Higher values can potentially
        /// give faster write performance at the cost of higher peak
        /// memory consumption
        pub max_buffered_batches_per_output_file: usize, default = 2

        /// Should sub directories be ignored when scanning directories for data
        /// files. Defaults to true (ignores subdirectories), consistent with
        /// Hive. Note that this setting does not affect reading partitioned
        /// tables (e.g. `/table/year=2021/month=01/data.parquet`).
        pub listing_table_ignore_subdirectory: bool, default = true

        /// Should a `ListingTable` created through the `ListingTableFactory` infer table
        /// partitions from Hive compliant directories. Defaults to true (partition columns are
        /// inferred and will be represented in the table schema).
        pub listing_table_factory_infer_partitions: bool, default = true

        /// Should DataFusion support recursive CTEs
        pub enable_recursive_ctes: bool, default = true

        /// Attempt to eliminate sorts by packing & sorting files with non-overlapping
        /// statistics into the same file groups.
        /// Currently experimental
        pub split_file_groups_by_statistics: bool, default = false

        /// Should DataFusion keep the columns used for partition_by in the output RecordBatches
        pub keep_partition_by_columns: bool, default = false

        /// Aggregation ratio (number of distinct groups / number of input rows)
        /// threshold for skipping partial aggregation. If the value is greater
        /// then partial aggregation will skip aggregation for further input
        pub skip_partial_aggregation_probe_ratio_threshold: f64, default = 0.8

        /// Number of input rows partial aggregation partition should process, before
        /// aggregation ratio check and trying to switch to skipping aggregation mode
        pub skip_partial_aggregation_probe_rows_threshold: usize, default = 100_000

        /// Should DataFusion use row number estimates at the input to decide
        /// whether increasing parallelism is beneficial or not. By default,
        /// only exact row numbers (not estimates) are used for this decision.
        /// Setting this flag to `true` will likely produce better plans.
        /// if the source of statistics is accurate.
        /// We plan to make this the default in the future.
        pub use_row_number_estimates_to_optimize_partitioning: bool, default = false

        /// Should DataFusion enforce batch size in joins or not. By default,
        /// DataFusion will not enforce batch size in joins. Enforcing batch size
        /// in joins can reduce memory usage when joining large
        /// tables with a highly-selective join filter, but is also slightly slower.
        pub enforce_batch_size_in_joins: bool, default = false

        /// Size (bytes) of data buffer DataFusion uses when writing output files.
        /// This affects the size of the data chunks that are uploaded to remote
        /// object stores (e.g. AWS S3). If very large (>= 100 GiB) output files are being
        /// written, it may be necessary to increase this size to avoid errors from
        /// the remote end point.
        pub objectstore_writer_buffer_size: usize, default = 10 * 1024 * 1024

        /// Whether to enable ANSI SQL mode.
        ///
        /// The flag is experimental and relevant only for DataFusion Spark built-in functions
        ///
        /// When `enable_ansi_mode` is set to `true`, the query engine follows ANSI SQL
        /// semantics for expressions, casting, and error handling. This means:
        /// - **Strict type coercion rules:** implicit casts between incompatible types are disallowed.
        /// - **Standard SQL arithmetic behavior:** operations such as division by zero,
        ///   numeric overflow, or invalid casts raise runtime errors rather than returning
        ///   `NULL` or adjusted values.
        /// - **Consistent ANSI behavior** for string concatenation, comparisons, and `NULL` handling.
        ///
        /// When `enable_ansi_mode` is `false` (the default), the engine uses a more permissive,
        /// non-ANSI mode designed for user convenience and backward compatibility. In this mode:
        /// - Implicit casts between types are allowed (e.g., string to integer when possible).
        /// - Arithmetic operations are more lenient — for example, `abs()` on the minimum
        ///   representable integer value returns the input value instead of raising overflow.
        /// - Division by zero or invalid casts may return `NULL` instead of failing.
        ///
        /// # Default
        /// `false` — ANSI SQL mode is disabled by default.
        pub enable_ansi_mode: bool, default = false
    }
}

config_namespace! {
    /// Options for reading and writing parquet files
    ///
    /// See also: [`SessionConfig`]
    ///
    /// [`SessionConfig`]: https://docs.rs/datafusion/latest/datafusion/prelude/struct.SessionConfig.html
    pub struct ParquetOptions {
        // The following options affect reading parquet files

        /// (reading) If true, reads the Parquet data page level metadata (the
        /// Page Index), if present, to reduce the I/O and number of
        /// rows decoded.
        pub enable_page_index: bool, default = true

        /// (reading) If true, the parquet reader attempts to skip entire row groups based
        /// on the predicate in the query and the metadata (min/max values) stored in
        /// the parquet file
        pub pruning: bool, default = true

        /// (reading) If true, the parquet reader skip the optional embedded metadata that may be in
        /// the file Schema. This setting can help avoid schema conflicts when querying
        /// multiple parquet files with schemas containing compatible types but different metadata
        pub skip_metadata: bool, default = true

        /// (reading) If specified, the parquet reader will try and fetch the last `size_hint`
        /// bytes of the parquet file optimistically. If not specified, two reads are required:
        /// One read to fetch the 8-byte parquet footer and
        /// another to fetch the metadata length encoded in the footer
        /// Default setting to 512 KiB, which should be sufficient for most parquet files,
        /// it can reduce one I/O operation per parquet file. If the metadata is larger than
        /// the hint, two reads will still be performed.
        pub metadata_size_hint: Option<usize>, default = Some(512 * 1024)

        /// (reading) If true, filter expressions are be applied during the parquet decoding operation to
        /// reduce the number of rows decoded. This optimization is sometimes called "late materialization".
        pub pushdown_filters: bool, default = false

        /// (reading) If true, filter expressions evaluated during the parquet decoding operation
        /// will be reordered heuristically to minimize the cost of evaluation. If false,
        /// the filters are applied in the same order as written in the query
        pub reorder_filters: bool, default = false

        /// (reading) If true, parquet reader will read columns of `Utf8/Utf8Large` with `Utf8View`,
        /// and `Binary/BinaryLarge` with `BinaryView`.
        pub schema_force_view_types: bool, default = true

        /// (reading) If true, parquet reader will read columns of
        /// `Binary/LargeBinary` with `Utf8`, and `BinaryView` with `Utf8View`.
        ///
        /// Parquet files generated by some legacy writers do not correctly set
        /// the UTF8 flag for strings, causing string columns to be loaded as
        /// BLOB instead.
        pub binary_as_string: bool, default = false

        /// (reading) If true, parquet reader will read columns of
        /// physical type int96 as originating from a different resolution
        /// than nanosecond. This is useful for reading data from systems like Spark
        /// which stores microsecond resolution timestamps in an int96 allowing it
        /// to write values with a larger date range than 64-bit timestamps with
        /// nanosecond resolution.
        pub coerce_int96: Option<String>, transform = str::to_lowercase, default = None

        /// (reading) Use any available bloom filters when reading parquet files
        pub bloom_filter_on_read: bool, default = true

        /// (reading) The maximum predicate cache size, in bytes. When
        /// `pushdown_filters` is enabled, sets the maximum memory used to cache
        /// the results of predicate evaluation between filter evaluation and
        /// output generation. Decreasing this value will reduce memory usage,
        /// but may increase IO and CPU usage. None means use the default
        /// parquet reader setting. 0 means no caching.
        pub max_predicate_cache_size: Option<usize>, default = None

        // The following options affect writing to parquet files
        // and map to parquet::file::properties::WriterProperties

        /// (writing) Sets best effort maximum size of data page in bytes
        pub data_pagesize_limit: usize, default = 1024 * 1024

        /// (writing) Sets write_batch_size in bytes
        pub write_batch_size: usize, default = 1024

        /// (writing) Sets parquet writer version
        /// valid values are "1.0" and "2.0"
        pub writer_version: String, default = "1.0".to_string()

        /// (writing) Skip encoding the embedded arrow metadata in the KV_meta
        ///
        /// This is analogous to the `ArrowWriterOptions::with_skip_arrow_metadata`.
        /// Refer to <https://docs.rs/parquet/53.3.0/parquet/arrow/arrow_writer/struct.ArrowWriterOptions.html#method.with_skip_arrow_metadata>
        pub skip_arrow_metadata: bool, default = false

        /// (writing) Sets default parquet compression codec.
        /// Valid values are: uncompressed, snappy, gzip(level),
        /// lzo, brotli(level), lz4, zstd(level), and lz4_raw.
        /// These values are not case sensitive. If NULL, uses
        /// default parquet writer setting
        ///
        /// Note that this default setting is not the same as
        /// the default parquet writer setting.
        pub compression: Option<String>, transform = str::to_lowercase, default = Some("zstd(3)".into())

        /// (writing) Sets if dictionary encoding is enabled. If NULL, uses
        /// default parquet writer setting
        pub dictionary_enabled: Option<bool>, default = Some(true)

        /// (writing) Sets best effort maximum dictionary page size, in bytes
        pub dictionary_page_size_limit: usize, default = 1024 * 1024

        /// (writing) Sets if statistics are enabled for any column
        /// Valid values are: "none", "chunk", and "page"
        /// These values are not case sensitive. If NULL, uses
        /// default parquet writer setting
        pub statistics_enabled: Option<String>, transform = str::to_lowercase, default = Some("page".into())

        /// (writing) Target maximum number of rows in each row group (defaults to 1M
        /// rows). Writing larger row groups requires more memory to write, but
        /// can get better compression and be faster to read.
        pub max_row_group_size: usize, default =  1024 * 1024

        /// (writing) Sets "created by" property
        pub created_by: String, default = concat!("datafusion version ", env!("CARGO_PKG_VERSION")).into()

        /// (writing) Sets column index truncate length
        pub column_index_truncate_length: Option<usize>, default = Some(64)

        /// (writing) Sets statistics truncate length. If NULL, uses
        /// default parquet writer setting
        pub statistics_truncate_length: Option<usize>, default = Some(64)

        /// (writing) Sets best effort maximum number of rows in data page
        pub data_page_row_count_limit: usize, default = 20_000

        /// (writing)  Sets default encoding for any column.
        /// Valid values are: plain, plain_dictionary, rle,
        /// bit_packed, delta_binary_packed, delta_length_byte_array,
        /// delta_byte_array, rle_dictionary, and byte_stream_split.
        /// These values are not case sensitive. If NULL, uses
        /// default parquet writer setting
        pub encoding: Option<String>, transform = str::to_lowercase, default = None

        /// (writing) Write bloom filters for all columns when creating parquet files
        pub bloom_filter_on_write: bool, default = false

        /// (writing) Sets bloom filter false positive probability. If NULL, uses
        /// default parquet writer setting
        pub bloom_filter_fpp: Option<f64>, default = None

        /// (writing) Sets bloom filter number of distinct values. If NULL, uses
        /// default parquet writer setting
        pub bloom_filter_ndv: Option<u64>, default = None

        /// (writing) Controls whether DataFusion will attempt to speed up writing
        /// parquet files by serializing them in parallel. Each column
        /// in each row group in each output file are serialized in parallel
        /// leveraging a maximum possible core count of n_files*n_row_groups*n_columns.
        pub allow_single_file_parallelism: bool, default = true

        /// (writing) By default parallel parquet writer is tuned for minimum
        /// memory usage in a streaming execution plan. You may see
        /// a performance benefit when writing large parquet files
        /// by increasing maximum_parallel_row_group_writers and
        /// maximum_buffered_record_batches_per_stream if your system
        /// has idle cores and can tolerate additional memory usage.
        /// Boosting these values is likely worthwhile when
        /// writing out already in-memory data, such as from a cached
        /// data frame.
        pub maximum_parallel_row_group_writers: usize, default = 1

        /// (writing) By default parallel parquet writer is tuned for minimum
        /// memory usage in a streaming execution plan. You may see
        /// a performance benefit when writing large parquet files
        /// by increasing maximum_parallel_row_group_writers and
        /// maximum_buffered_record_batches_per_stream if your system
        /// has idle cores and can tolerate additional memory usage.
        /// Boosting these values is likely worthwhile when
        /// writing out already in-memory data, such as from a cached
        /// data frame.
        pub maximum_buffered_record_batches_per_stream: usize, default = 2
    }
}

config_namespace! {
    /// Options for configuring Parquet Modular Encryption
    ///
    /// To use Parquet encryption, you must enable the `parquet_encryption` feature flag, as it is not activated by default.
    pub struct ParquetEncryptionOptions {
        /// Optional file decryption properties
        pub file_decryption: Option<ConfigFileDecryptionProperties>, default = None

        /// Optional file encryption properties
        pub file_encryption: Option<ConfigFileEncryptionProperties>, default = None

        /// Identifier for the encryption factory to use to create file encryption and decryption properties.
        /// Encryption factories can be registered in the runtime environment with
        /// `RuntimeEnv::register_parquet_encryption_factory`.
        pub factory_id: Option<String>, default = None

        /// Any encryption factory specific options
        pub factory_options: EncryptionFactoryOptions, default = EncryptionFactoryOptions::default()
    }
}

impl ParquetEncryptionOptions {
    /// Specify the encryption factory to use for Parquet modular encryption, along with its configuration
    pub fn configure_factory(
        &mut self,
        factory_id: &str,
        config: &impl ExtensionOptions,
    ) {
        self.factory_id = Some(factory_id.to_owned());
        self.factory_options.options.clear();
        for entry in config.entries() {
            if let Some(value) = entry.value {
                self.factory_options.options.insert(entry.key, value);
            }
        }
    }
}

config_namespace! {
    /// Options related to query optimization
    ///
    /// See also: [`SessionConfig`]
    ///
    /// [`SessionConfig`]: https://docs.rs/datafusion/latest/datafusion/prelude/struct.SessionConfig.html
    pub struct OptimizerOptions {
        /// When set to true, the optimizer will push a limit operation into
        /// grouped aggregations which have no aggregate expressions, as a soft limit,
        /// emitting groups once the limit is reached, before all rows in the group are read.
        pub enable_distinct_aggregation_soft_limit: bool, default = true

        /// When set to true, the physical plan optimizer will try to add round robin
        /// repartitioning to increase parallelism to leverage more CPU cores
        pub enable_round_robin_repartition: bool, default = true

        /// When set to true, the optimizer will attempt to perform limit operations
        /// during aggregations, if possible
        pub enable_topk_aggregation: bool, default = true

        /// When set to true, the optimizer will attempt to push limit operations
        /// past window functions, if possible
        pub enable_window_limits: bool, default = true

        /// When set to true, the optimizer will attempt to push down TopK dynamic filters
        /// into the file scan phase.
        pub enable_topk_dynamic_filter_pushdown: bool, default = true

        /// When set to true, the optimizer will attempt to push down Join dynamic filters
        /// into the file scan phase.
        pub enable_join_dynamic_filter_pushdown: bool, default = true

        /// When set to true attempts to push down dynamic filters generated by operators (topk & join) into the file scan phase.
        /// For example, for a query such as `SELECT * FROM t ORDER BY timestamp DESC LIMIT 10`, the optimizer
        /// will attempt to push down the current top 10 timestamps that the TopK operator references into the file scans.
        /// This means that if we already have 10 timestamps in the year 2025
        /// any files that only have timestamps in the year 2024 can be skipped / pruned at various stages in the scan.
        /// The config will suppress `enable_join_dynamic_filter_pushdown` & `enable_topk_dynamic_filter_pushdown`
        /// So if you disable `enable_topk_dynamic_filter_pushdown`, then enable `enable_dynamic_filter_pushdown`, the `enable_topk_dynamic_filter_pushdown` will be overridden.
        pub enable_dynamic_filter_pushdown: bool, default = true

        /// When set to true, the optimizer will insert filters before a join between
        /// a nullable and non-nullable column to filter out nulls on the nullable side. This
        /// filter can add additional overhead when the file format does not fully support
        /// predicate push down.
        pub filter_null_join_keys: bool, default = false

        /// Should DataFusion repartition data using the aggregate keys to execute aggregates
        /// in parallel using the provided `target_partitions` level
        pub repartition_aggregations: bool, default = true

        /// Minimum total files size in bytes to perform file scan repartitioning.
        pub repartition_file_min_size: usize, default = 10 * 1024 * 1024

        /// Should DataFusion repartition data using the join keys to execute joins in parallel
        /// using the provided `target_partitions` level
        pub repartition_joins: bool, default = true

        /// Should DataFusion allow symmetric hash joins for unbounded data sources even when
        /// its inputs do not have any ordering or filtering If the flag is not enabled,
        /// the SymmetricHashJoin operator will be unable to prune its internal buffers,
        /// resulting in certain join types - such as Full, Left, LeftAnti, LeftSemi, Right,
        /// RightAnti, and RightSemi - being produced only at the end of the execution.
        /// This is not typical in stream processing. Additionally, without proper design for
        /// long runner execution, all types of joins may encounter out-of-memory errors.
        pub allow_symmetric_joins_without_pruning: bool, default = true

        /// When set to `true`, datasource partitions will be repartitioned to achieve maximum parallelism.
        /// This applies to both in-memory partitions and FileSource's file groups (1 group is 1 partition).
        ///
        /// For FileSources, only Parquet and CSV formats are currently supported.
        ///
        /// If set to `true` for a FileSource, all files will be repartitioned evenly (i.e., a single large file
        /// might be partitioned into smaller chunks) for parallel scanning.
        /// If set to `false` for a FileSource, different files will be read in parallel, but repartitioning won't
        /// happen within a single file.
        ///
        /// If set to `true` for an in-memory source, all memtable's partitions will have their batches
        /// repartitioned evenly to the desired number of `target_partitions`. Repartitioning can change
        /// the total number of partitions and batches per partition, but does not slice the initial
        /// record tables provided to the MemTable on creation.
        pub repartition_file_scans: bool, default = true

        /// Should DataFusion repartition data using the partitions keys to execute window
        /// functions in parallel using the provided `target_partitions` level
        pub repartition_windows: bool, default = true

        /// Should DataFusion execute sorts in a per-partition fashion and merge
        /// afterwards instead of coalescing first and sorting globally.
        /// With this flag is enabled, plans in the form below
        ///
        /// ```text
        ///      "SortExec: [a@0 ASC]",
        ///      "  CoalescePartitionsExec",
        ///      "    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
        /// ```
        /// would turn into the plan below which performs better in multithreaded environments
        ///
        /// ```text
        ///      "SortPreservingMergeExec: [a@0 ASC]",
        ///      "  SortExec: [a@0 ASC]",
        ///      "    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
        /// ```
        pub repartition_sorts: bool, default = true

        /// When true, DataFusion will opportunistically remove sorts when the data is already sorted,
        /// (i.e. setting `preserve_order` to true on `RepartitionExec`  and
        /// using `SortPreservingMergeExec`)
        ///
        /// When false, DataFusion will maximize plan parallelism using
        /// `RepartitionExec` even if this requires subsequently resorting data using a `SortExec`.
        pub prefer_existing_sort: bool, default = false

        /// When set to true, the logical plan optimizer will produce warning
        /// messages if any optimization rules produce errors and then proceed to the next
        /// rule. When set to false, any rules that produce errors will cause the query to fail
        pub skip_failed_rules: bool, default = false

        /// Number of times that the optimizer will attempt to optimize the plan
        pub max_passes: usize, default = 3

        /// When set to true, the physical plan optimizer will run a top down
        /// process to reorder the join keys
        pub top_down_join_key_reordering: bool, default = true

        /// When set to true, the physical plan optimizer will prefer HashJoin over SortMergeJoin.
        /// HashJoin can work more efficiently than SortMergeJoin but consumes more memory
        pub prefer_hash_join: bool, default = true

        /// When set to true, piecewise merge join is enabled. PiecewiseMergeJoin is currently
        /// experimental. Physical planner will opt for PiecewiseMergeJoin when there is only
        /// one range filter.
        pub enable_piecewise_merge_join: bool, default = false

        /// The maximum estimated size in bytes for one input side of a HashJoin
        /// will be collected into a single partition
        pub hash_join_single_partition_threshold: usize, default = 1024 * 1024

        /// The maximum estimated size in rows for one input side of a HashJoin
        /// will be collected into a single partition
        pub hash_join_single_partition_threshold_rows: usize, default = 1024 * 128

        /// The default filter selectivity used by Filter Statistics
        /// when an exact selectivity cannot be determined. Valid values are
        /// between 0 (no selectivity) and 100 (all rows are selected).
        pub default_filter_selectivity: u8, default = 20

        /// When set to true, the optimizer will not attempt to convert Union to Interleave
        pub prefer_existing_union: bool, default = false

        /// When set to true, if the returned type is a view type
        /// then the output will be coerced to a non-view.
        /// Coerces `Utf8View` to `LargeUtf8`, and `BinaryView` to `LargeBinary`.
        pub expand_views_at_output: bool, default = false
    }
}

config_namespace! {
    /// Options controlling explain output
    ///
    /// See also: [`SessionConfig`]
    ///
    /// [`SessionConfig`]: https://docs.rs/datafusion/latest/datafusion/prelude/struct.SessionConfig.html
    pub struct ExplainOptions {
        /// When set to true, the explain statement will only print logical plans
        pub logical_plan_only: bool, default = false

        /// When set to true, the explain statement will only print physical plans
        pub physical_plan_only: bool, default = false

        /// When set to true, the explain statement will print operator statistics
        /// for physical plans
        pub show_statistics: bool, default = false

        /// When set to true, the explain statement will print the partition sizes
        pub show_sizes: bool, default = true

        /// When set to true, the explain statement will print schema information
        pub show_schema: bool, default = false

        /// Display format of explain. Default is "indent".
        /// When set to "tree", it will print the plan in a tree-rendered format.
        pub format: ExplainFormat, default = ExplainFormat::Indent

        /// (format=tree only) Maximum total width of the rendered tree.
        /// When set to 0, the tree will have no width limit.
        pub tree_maximum_render_width: usize, default = 240

        /// Verbosity level for "EXPLAIN ANALYZE". Default is "dev"
        /// "summary" shows common metrics for high-level insights.
        /// "dev" provides deep operator-level introspection for developers.
        pub analyze_level: ExplainAnalyzeLevel, default = ExplainAnalyzeLevel::Dev
    }
}

impl ExecutionOptions {
    /// Returns the correct parallelism based on the provided `value`.
    /// If `value` is `"0"`, returns the default available parallelism, computed with
    /// `get_available_parallelism`. Otherwise, returns `value`.
    fn normalized_parallelism(value: &str) -> String {
        if value.parse::<usize>() == Ok(0) {
            get_available_parallelism().to_string()
        } else {
            value.to_owned()
        }
    }
}

config_namespace! {
    /// Options controlling the format of output when printing record batches
    /// Copies [`arrow::util::display::FormatOptions`]
    pub struct FormatOptions {
        /// If set to `true` any formatting errors will be written to the output
        /// instead of being converted into a [`std::fmt::Error`]
        pub safe: bool, default = true
        /// Format string for nulls
        pub null: String, default = "".into()
        /// Date format for date arrays
        pub date_format: Option<String>, default = Some("%Y-%m-%d".to_string())
        /// Format for DateTime arrays
        pub datetime_format: Option<String>, default = Some("%Y-%m-%dT%H:%M:%S%.f".to_string())
        /// Timestamp format for timestamp arrays
        pub timestamp_format: Option<String>, default = Some("%Y-%m-%dT%H:%M:%S%.f".to_string())
        /// Timestamp format for timestamp with timezone arrays. When `None`, ISO 8601 format is used.
        pub timestamp_tz_format: Option<String>, default = None
        /// Time format for time arrays
        pub time_format: Option<String>, default = Some("%H:%M:%S%.f".to_string())
        /// Duration format. Can be either `"pretty"` or `"ISO8601"`
        pub duration_format: String, transform = str::to_lowercase, default = "pretty".into()
        /// Show types in visual representation batches
        pub types_info: bool, default = false
    }
}

impl<'a> TryInto<arrow::util::display::FormatOptions<'a>> for &'a FormatOptions {
    type Error = DataFusionError;
    fn try_into(self) -> Result<arrow::util::display::FormatOptions<'a>> {
        let duration_format = match self.duration_format.as_str() {
            "pretty" => arrow::util::display::DurationFormat::Pretty,
            "iso8601" => arrow::util::display::DurationFormat::ISO8601,
            _ => {
                return _config_err!(
                    "Invalid duration format: {}. Valid values are pretty or iso8601",
                    self.duration_format
                );
            }
        };

        Ok(arrow::util::display::FormatOptions::new()
            .with_display_error(self.safe)
            .with_null(&self.null)
            .with_date_format(self.date_format.as_deref())
            .with_datetime_format(self.datetime_format.as_deref())
            .with_timestamp_format(self.timestamp_format.as_deref())
            .with_timestamp_tz_format(self.timestamp_tz_format.as_deref())
            .with_time_format(self.time_format.as_deref())
            .with_duration_format(duration_format)
            .with_types_info(self.types_info))
    }
}

/// A key value pair, with a corresponding description
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct ConfigEntry {
    /// A unique string to identify this config value
    pub key: String,

    /// The value if any
    pub value: Option<String>,

    /// A description of this configuration entry
    pub description: &'static str,
}

/// Configuration options struct, able to store both built-in configuration and custom options
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct ConfigOptions {
    /// Catalog options
    pub catalog: CatalogOptions,
    /// Execution options
    pub execution: ExecutionOptions,
    /// Optimizer options
    pub optimizer: OptimizerOptions,
    /// SQL parser options
    pub sql_parser: SqlParserOptions,
    /// Explain options
    pub explain: ExplainOptions,
    /// Optional extensions registered using [`Extensions::insert`]
    pub extensions: Extensions,
    /// Formatting options when printing batches
    pub format: FormatOptions,
}

impl ConfigField for ConfigOptions {
    fn visit<V: Visit>(&self, v: &mut V, _key_prefix: &str, _description: &'static str) {
        self.catalog.visit(v, "datafusion.catalog", "");
        self.execution.visit(v, "datafusion.execution", "");
        self.optimizer.visit(v, "datafusion.optimizer", "");
        self.explain.visit(v, "datafusion.explain", "");
        self.sql_parser.visit(v, "datafusion.sql_parser", "");
        self.format.visit(v, "datafusion.format", "");
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        // Extensions are handled in the public `ConfigOptions::set`
        let (key, rem) = key.split_once('.').unwrap_or((key, ""));
        match key {
            "catalog" => self.catalog.set(rem, value),
            "execution" => self.execution.set(rem, value),
            "optimizer" => self.optimizer.set(rem, value),
            "explain" => self.explain.set(rem, value),
            "sql_parser" => self.sql_parser.set(rem, value),
            "format" => self.format.set(rem, value),
            _ => _config_err!("Config value \"{key}\" not found on ConfigOptions"),
        }
    }

    /// Reset a configuration option back to its default value
    fn reset(&mut self, key: &str) -> Result<()> {
        let Some((prefix, rest)) = key.split_once('.') else {
            return _config_err!("could not find config namespace for key \"{key}\"");
        };

        if prefix != "datafusion" {
            return _config_err!("Could not find config namespace \"{prefix}\"");
        }

        let (section, rem) = rest.split_once('.').unwrap_or((rest, ""));
        if rem.is_empty() {
            return _config_err!("could not find config field for key \"{key}\"");
        }

        match section {
            "catalog" => self.catalog.reset(rem),
            "execution" => self.execution.reset(rem),
            "optimizer" => {
                if rem == "enable_dynamic_filter_pushdown" {
                    let defaults = OptimizerOptions::default();
                    self.optimizer.enable_dynamic_filter_pushdown =
                        defaults.enable_dynamic_filter_pushdown;
                    self.optimizer.enable_topk_dynamic_filter_pushdown =
                        defaults.enable_topk_dynamic_filter_pushdown;
                    self.optimizer.enable_join_dynamic_filter_pushdown =
                        defaults.enable_join_dynamic_filter_pushdown;
                    Ok(())
                } else {
                    self.optimizer.reset(rem)
                }
            }
            "explain" => self.explain.reset(rem),
            "sql_parser" => self.sql_parser.reset(rem),
            "format" => self.format.reset(rem),
            other => _config_err!("Config value \"{other}\" not found on ConfigOptions"),
        }
    }
}

impl ConfigOptions {
    /// Creates a new [`ConfigOptions`] with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set extensions to provided value
    pub fn with_extensions(mut self, extensions: Extensions) -> Self {
        self.extensions = extensions;
        self
    }

    /// Set a configuration option
    pub fn set(&mut self, key: &str, value: &str) -> Result<()> {
        let Some((prefix, key)) = key.split_once('.') else {
            return _config_err!("could not find config namespace for key \"{key}\"");
        };

        if prefix == "datafusion" {
            if key == "optimizer.enable_dynamic_filter_pushdown" {
                let bool_value = value.parse::<bool>().map_err(|e| {
                    DataFusionError::Configuration(format!(
                        "Failed to parse '{value}' as bool: {e}",
                    ))
                })?;

                {
                    self.optimizer.enable_dynamic_filter_pushdown = bool_value;
                    self.optimizer.enable_topk_dynamic_filter_pushdown = bool_value;
                    self.optimizer.enable_join_dynamic_filter_pushdown = bool_value;
                }
                return Ok(());
            }
            return ConfigField::set(self, key, value);
        }

        let Some(e) = self.extensions.0.get_mut(prefix) else {
            return _config_err!("Could not find config namespace \"{prefix}\"");
        };
        e.0.set(key, value)
    }

    /// Create new [`ConfigOptions`], taking values from environment variables
    /// where possible.
    ///
    /// For example, to configure `datafusion.execution.batch_size`
    /// ([`ExecutionOptions::batch_size`]) you would set the
    /// `DATAFUSION_EXECUTION_BATCH_SIZE` environment variable.
    ///
    /// The name of the environment variable is the option's key, transformed to
    /// uppercase and with periods replaced with underscores.
    ///
    /// Values are parsed according to the [same rules used in casts from
    /// Utf8](https://docs.rs/arrow/latest/arrow/compute/kernels/cast/fn.cast.html).
    ///
    /// If the value in the environment variable cannot be cast to the type of
    /// the configuration option, the default value will be used instead and a
    /// warning emitted. Environment variables are read when this method is
    /// called, and are not re-read later.
    pub fn from_env() -> Result<Self> {
        struct Visitor(Vec<String>);

        impl Visit for Visitor {
            fn some<V: Display>(&mut self, key: &str, _: V, _: &'static str) {
                self.0.push(key.to_string())
            }

            fn none(&mut self, key: &str, _: &'static str) {
                self.0.push(key.to_string())
            }
        }

        // Extract the names of all fields and then look up the corresponding
        // environment variables. This isn't hugely efficient but avoids
        // ambiguity between `a.b` and `a_b` which would both correspond
        // to an environment variable of `A_B`

        let mut keys = Visitor(vec![]);
        let mut ret = Self::default();
        ret.visit(&mut keys, "datafusion", "");

        for key in keys.0 {
            let env = key.to_uppercase().replace('.', "_");
            if let Some(var) = std::env::var_os(env) {
                let value = var.to_string_lossy();
                log::info!("Set {key} to {value} from the environment variable");
                ret.set(&key, value.as_ref())?;
            }
        }

        Ok(ret)
    }

    /// Create new ConfigOptions struct, taking values from a string hash map.
    ///
    /// Only the built-in configurations will be extracted from the hash map
    /// and other key value pairs will be ignored.
    pub fn from_string_hash_map(settings: &HashMap<String, String>) -> Result<Self> {
        struct Visitor(Vec<String>);

        impl Visit for Visitor {
            fn some<V: Display>(&mut self, key: &str, _: V, _: &'static str) {
                self.0.push(key.to_string())
            }

            fn none(&mut self, key: &str, _: &'static str) {
                self.0.push(key.to_string())
            }
        }

        let mut keys = Visitor(vec![]);
        let mut ret = Self::default();
        ret.visit(&mut keys, "datafusion", "");

        for key in keys.0 {
            if let Some(var) = settings.get(&key) {
                ret.set(&key, var)?;
            }
        }

        Ok(ret)
    }

    /// Returns the [`ConfigEntry`] stored within this [`ConfigOptions`]
    pub fn entries(&self) -> Vec<ConfigEntry> {
        struct Visitor(Vec<ConfigEntry>);

        impl Visit for Visitor {
            fn some<V: Display>(
                &mut self,
                key: &str,
                value: V,
                description: &'static str,
            ) {
                self.0.push(ConfigEntry {
                    key: key.to_string(),
                    value: Some(value.to_string()),
                    description,
                })
            }

            fn none(&mut self, key: &str, description: &'static str) {
                self.0.push(ConfigEntry {
                    key: key.to_string(),
                    value: None,
                    description,
                })
            }
        }

        let mut v = Visitor(vec![]);
        self.visit(&mut v, "datafusion", "");

        v.0.extend(self.extensions.0.values().flat_map(|e| e.0.entries()));
        v.0
    }

    /// Generate documentation that can be included in the user guide
    pub fn generate_config_markdown() -> String {
        use std::fmt::Write as _;

        let mut s = Self::default();

        // Normalize for display
        s.execution.target_partitions = 0;
        s.execution.planning_concurrency = 0;

        let mut docs = "| key | default | description |\n".to_string();
        docs += "|-----|---------|-------------|\n";
        let mut entries = s.entries();
        entries.sort_unstable_by(|a, b| a.key.cmp(&b.key));

        for entry in s.entries() {
            let _ = writeln!(
                &mut docs,
                "| {} | {} | {} |",
                entry.key,
                entry.value.as_deref().unwrap_or("NULL"),
                entry.description
            );
        }
        docs
    }
}

/// [`ConfigExtension`] provides a mechanism to store third-party configuration
/// within DataFusion [`ConfigOptions`]
///
/// This mechanism can be used to pass configuration to user defined functions
/// or optimizer passes
///
/// # Example
/// ```
/// use datafusion_common::{
///     config::ConfigExtension, config::ConfigOptions, extensions_options,
/// };
/// // Define a new configuration struct using the `extensions_options` macro
/// extensions_options! {
///    /// My own config options.
///    pub struct MyConfig {
///        /// Should "foo" be replaced by "bar"?
///        pub foo_to_bar: bool, default = true
///
///        /// How many "baz" should be created?
///        pub baz_count: usize, default = 1337
///    }
/// }
///
/// impl ConfigExtension for MyConfig {
///     const PREFIX: &'static str = "my_config";
/// }
///
/// // set up config struct and register extension
/// let mut config = ConfigOptions::default();
/// config.extensions.insert(MyConfig::default());
///
/// // overwrite config default
/// config.set("my_config.baz_count", "42").unwrap();
///
/// // check config state
/// let my_config = config.extensions.get::<MyConfig>().unwrap();
/// assert!(my_config.foo_to_bar,);
/// assert_eq!(my_config.baz_count, 42,);
/// ```
///
/// # Note:
/// Unfortunately associated constants are not currently object-safe, and so this
/// extends the object-safe [`ExtensionOptions`]
pub trait ConfigExtension: ExtensionOptions {
    /// Configuration namespace prefix to use
    ///
    /// All values under this will be prefixed with `$PREFIX + "."`
    const PREFIX: &'static str;
}

/// An object-safe API for storing arbitrary configuration.
///
/// See [`ConfigExtension`] for user defined configuration
pub trait ExtensionOptions: Send + Sync + fmt::Debug + 'static {
    /// Return `self` as [`Any`]
    ///
    /// This is needed until trait upcasting is stabilized
    fn as_any(&self) -> &dyn Any;

    /// Return `self` as [`Any`]
    ///
    /// This is needed until trait upcasting is stabilized
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// Return a deep clone of this [`ExtensionOptions`]
    ///
    /// It is important this does not share mutable state to avoid consistency issues
    /// with configuration changing whilst queries are executing
    fn cloned(&self) -> Box<dyn ExtensionOptions>;

    /// Set the given `key`, `value` pair
    fn set(&mut self, key: &str, value: &str) -> Result<()>;

    /// Returns the [`ConfigEntry`] stored in this [`ExtensionOptions`]
    fn entries(&self) -> Vec<ConfigEntry>;
}

/// A type-safe container for [`ConfigExtension`]
#[derive(Debug, Default, Clone)]
pub struct Extensions(BTreeMap<&'static str, ExtensionBox>);

impl Extensions {
    /// Create a new, empty [`Extensions`]
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    /// Registers a [`ConfigExtension`] with this [`ConfigOptions`]
    pub fn insert<T: ConfigExtension>(&mut self, extension: T) {
        assert_ne!(T::PREFIX, "datafusion");
        let e = ExtensionBox(Box::new(extension));
        self.0.insert(T::PREFIX, e);
    }

    /// Retrieves the extension of the given type if any
    pub fn get<T: ConfigExtension>(&self) -> Option<&T> {
        self.0.get(T::PREFIX)?.0.as_any().downcast_ref()
    }

    /// Retrieves the extension of the given type if any
    pub fn get_mut<T: ConfigExtension>(&mut self) -> Option<&mut T> {
        let e = self.0.get_mut(T::PREFIX)?;
        e.0.as_any_mut().downcast_mut()
    }

    /// Iterates all the config extension entries yielding their prefix and their
    /// [ExtensionOptions] implementation.
    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (&'static str, &Box<dyn ExtensionOptions>)> {
        self.0.iter().map(|(k, v)| (*k, &v.0))
    }
}

#[derive(Debug)]
struct ExtensionBox(Box<dyn ExtensionOptions>);

impl Clone for ExtensionBox {
    fn clone(&self) -> Self {
        Self(self.0.cloned())
    }
}

/// A trait implemented by `config_namespace` and for field types that provides
/// the ability to walk and mutate the configuration tree
pub trait ConfigField {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str);

    fn set(&mut self, key: &str, value: &str) -> Result<()>;

    fn reset(&mut self, key: &str) -> Result<()> {
        _config_err!("Reset is not supported for this config field, key: {}", key)
    }
}

impl<F: ConfigField + Default> ConfigField for Option<F> {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        match self {
            Some(s) => s.visit(v, key, description),
            None => v.none(key, description),
        }
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        self.get_or_insert_with(Default::default).set(key, value)
    }

    fn reset(&mut self, key: &str) -> Result<()> {
        if key.is_empty() {
            *self = Default::default();
            Ok(())
        } else {
            self.get_or_insert_with(Default::default).reset(key)
        }
    }
}

/// Default transformation to parse a [`ConfigField`] for a string.
///
/// This uses [`FromStr`] to parse the data.
pub fn default_config_transform<T>(input: &str) -> Result<T>
where
    T: FromStr,
    <T as FromStr>::Err: Sync + Send + Error + 'static,
{
    input.parse().map_err(|e| {
        DataFusionError::Context(
            format!(
                "Error parsing '{}' as {}",
                input,
                std::any::type_name::<T>()
            ),
            Box::new(DataFusionError::External(Box::new(e))),
        )
    })
}

/// Macro that generates [`ConfigField`] for a given type.
///
/// # Usage
/// This always requires [`Display`] to be implemented for the given type.
///
/// There are two ways to invoke this macro. The first one uses
/// [`default_config_transform`]/[`FromStr`] to parse the data:
///
/// ```ignore
/// config_field(MyType);
/// ```
///
/// Note that the parsing error MUST implement [`std::error::Error`]!
///
/// Or you can specify how you want to parse an [`str`] into the type:
///
/// ```ignore
/// fn parse_it(s: &str) -> Result<MyType> {
///     ...
/// }
///
/// config_field(
///     MyType,
///     value => parse_it(value)
/// );
/// ```
#[macro_export]
macro_rules! config_field {
    ($t:ty) => {
        config_field!($t, value => $crate::config::default_config_transform(value)?);
    };

    ($t:ty, $arg:ident => $transform:expr) => {
        impl $crate::config::ConfigField for $t {
            fn visit<V: $crate::config::Visit>(&self, v: &mut V, key: &str, description: &'static str) {
                v.some(key, self, description)
            }

            fn set(&mut self, _: &str, $arg: &str) -> $crate::error::Result<()> {
                *self = $transform;
                Ok(())
            }

            fn reset(&mut self, key: &str) -> $crate::error::Result<()> {
                if key.is_empty() {
                    *self = <$t as Default>::default();
                    Ok(())
                } else {
                    $crate::error::_config_err!(
                        "Config field is a scalar {} and does not have nested field \"{}\"",
                        stringify!($t),
                        key
                    )
                }
            }
        }
    };
}

config_field!(String);
config_field!(bool, value => default_config_transform(value.to_lowercase().as_str())?);
config_field!(usize);
config_field!(f64);
config_field!(u64);

impl ConfigField for u8 {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        v.some(key, self, description)
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        if value.is_empty() {
            return Err(DataFusionError::Configuration(format!(
                "Input string for {key} key is empty"
            )));
        }
        // Check if the string is a valid number
        if let Ok(num) = value.parse::<u8>() {
            // TODO: Let's decide how we treat the numerical strings.
            *self = num;
        } else {
            let bytes = value.as_bytes();
            // Check if the first character is ASCII (single byte)
            if bytes.len() > 1 || !value.chars().next().unwrap().is_ascii() {
                return Err(DataFusionError::Configuration(format!(
                    "Error parsing {value} as u8. Non-ASCII string provided"
                )));
            }
            *self = bytes[0];
        }
        Ok(())
    }
}

impl ConfigField for CompressionTypeVariant {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        v.some(key, self, description)
    }

    fn set(&mut self, _: &str, value: &str) -> Result<()> {
        *self = CompressionTypeVariant::from_str(value)?;
        Ok(())
    }
}

/// An implementation trait used to recursively walk configuration
pub trait Visit {
    fn some<V: Display>(&mut self, key: &str, value: V, description: &'static str);

    fn none(&mut self, key: &str, description: &'static str);
}

/// Convenience macro to create [`ExtensionsOptions`].
///
/// The created structure implements the following traits:
///
/// - [`Clone`]
/// - [`Debug`]
/// - [`Default`]
/// - [`ExtensionOptions`]
///
/// # Usage
/// The syntax is:
///
/// ```text
/// extensions_options! {
///      /// Struct docs (optional).
///     [<vis>] struct <StructName> {
///         /// Field docs (optional)
///         [<vis>] <field_name>: <field_type>, default = <default_value>
///
///         ... more fields
///     }
/// }
/// ```
///
/// The placeholders are:
/// - `[<vis>]`: Optional visibility modifier like `pub` or `pub(crate)`.
/// - `<StructName>`: Struct name like `MyStruct`.
/// - `<field_name>`: Field name like `my_field`.
/// - `<field_type>`: Field type like `u8`.
/// - `<default_value>`: Default value matching the field type like `42`.
///
/// # Example
/// See also a full example on the [`ConfigExtension`] documentation
///
/// ```
/// use datafusion_common::extensions_options;
///
/// extensions_options! {
///     /// My own config options.
///     pub struct MyConfig {
///         /// Should "foo" be replaced by "bar"?
///         pub foo_to_bar: bool, default = true
///
///         /// How many "baz" should be created?
///         pub baz_count: usize, default = 1337
///     }
/// }
/// ```
///
///
/// [`Debug`]: std::fmt::Debug
/// [`ExtensionsOptions`]: crate::config::ExtensionOptions
#[macro_export]
macro_rules! extensions_options {
    (
     $(#[doc = $struct_d:tt])*
     $vis:vis struct $struct_name:ident {
        $(
        $(#[doc = $d:tt])*
        $field_vis:vis $field_name:ident : $field_type:ty, default = $default:expr
        )*$(,)*
    }
    ) => {
        $(#[doc = $struct_d])*
        #[derive(Debug, Clone)]
        #[non_exhaustive]
        $vis struct $struct_name{
            $(
            $(#[doc = $d])*
            $field_vis $field_name : $field_type,
            )*
        }

        impl Default for $struct_name {
            fn default() -> Self {
                Self {
                    $($field_name: $default),*
                }
            }
        }

        impl $crate::config::ExtensionOptions for $struct_name {
            fn as_any(&self) -> &dyn ::std::any::Any {
                self
            }

            fn as_any_mut(&mut self) -> &mut dyn ::std::any::Any {
                self
            }

            fn cloned(&self) -> Box<dyn $crate::config::ExtensionOptions> {
                Box::new(self.clone())
            }

            fn set(&mut self, key: &str, value: &str) -> $crate::error::Result<()> {
                $crate::config::ConfigField::set(self, key, value)
            }

            fn entries(&self) -> Vec<$crate::config::ConfigEntry> {
                struct Visitor(Vec<$crate::config::ConfigEntry>);

                impl $crate::config::Visit for Visitor {
                    fn some<V: std::fmt::Display>(
                        &mut self,
                        key: &str,
                        value: V,
                        description: &'static str,
                    ) {
                        self.0.push($crate::config::ConfigEntry {
                            key: key.to_string(),
                            value: Some(value.to_string()),
                            description,
                        })
                    }

                    fn none(&mut self, key: &str, description: &'static str) {
                        self.0.push($crate::config::ConfigEntry {
                            key: key.to_string(),
                            value: None,
                            description,
                        })
                    }
                }

                let mut v = Visitor(vec![]);
                // The prefix is not used for extensions.
                // The description is generated in ConfigField::visit.
                // We can just pass empty strings here.
                $crate::config::ConfigField::visit(self, &mut v, "", "");
                v.0
            }
        }

        impl $crate::config::ConfigField for $struct_name {
            fn set(&mut self, key: &str, value: &str) -> $crate::error::Result<()> {
                let (key, rem) = key.split_once('.').unwrap_or((key, ""));
                match key {
                    $(
                        stringify!($field_name) => {
                            // Safely apply deprecated attribute if present
                            // $(#[allow(deprecated)])?
                            {
                                #[allow(deprecated)]
                                self.$field_name.set(rem, value.as_ref())
                            }
                        },
                    )*
                    _ => return $crate::error::_config_err!(
                        "Config value \"{}\" not found on {}", key, stringify!($struct_name)
                    )
                }
            }

            fn visit<V: $crate::config::Visit>(&self, v: &mut V, _key_prefix: &str, _description: &'static str) {
                $(
                    let key = stringify!($field_name).to_string();
                    let desc = concat!($($d),*).trim();
                    #[allow(deprecated)]
                    self.$field_name.visit(v, key.as_str(), desc);
                )*
            }
        }
    }
}

/// These file types have special built in behavior for configuration.
/// Use TableOptions::Extensions for configuring other file types.
#[derive(Debug, Clone)]
pub enum ConfigFileType {
    CSV,
    #[cfg(feature = "parquet")]
    PARQUET,
    JSON,
}

/// Represents the configuration options available for handling different table formats within a data processing application.
/// This struct encompasses options for various file formats including CSV, Parquet, and JSON, allowing for flexible configuration
/// of parsing and writing behaviors specific to each format. Additionally, it supports extending functionality through custom extensions.
#[derive(Debug, Clone, Default)]
pub struct TableOptions {
    /// Configuration options for CSV file handling. This includes settings like the delimiter,
    /// quote character, and whether the first row is considered as headers.
    pub csv: CsvOptions,

    /// Configuration options for Parquet file handling. This includes settings for compression,
    /// encoding, and other Parquet-specific file characteristics.
    pub parquet: TableParquetOptions,

    /// Configuration options for JSON file handling.
    pub json: JsonOptions,

    /// The current file format that the table operations should assume. This option allows
    /// for dynamic switching between the supported file types (e.g., CSV, Parquet, JSON).
    pub current_format: Option<ConfigFileType>,

    /// Optional extensions that can be used to extend or customize the behavior of the table
    /// options. Extensions can be registered using `Extensions::insert` and might include
    /// custom file handling logic, additional configuration parameters, or other enhancements.
    pub extensions: Extensions,
}

impl ConfigField for TableOptions {
    /// Visits configuration settings for the current file format, or all formats if none is selected.
    ///
    /// This method adapts the behavior based on whether a file format is currently selected in `current_format`.
    /// If a format is selected, it visits only the settings relevant to that format. Otherwise,
    /// it visits all available format settings.
    fn visit<V: Visit>(&self, v: &mut V, _key_prefix: &str, _description: &'static str) {
        if let Some(file_type) = &self.current_format {
            match file_type {
                #[cfg(feature = "parquet")]
                ConfigFileType::PARQUET => self.parquet.visit(v, "format", ""),
                ConfigFileType::CSV => self.csv.visit(v, "format", ""),
                ConfigFileType::JSON => self.json.visit(v, "format", ""),
            }
        } else {
            self.csv.visit(v, "csv", "");
            self.parquet.visit(v, "parquet", "");
            self.json.visit(v, "json", "");
        }
    }

    /// Sets a configuration value for a specific key within `TableOptions`.
    ///
    /// This method delegates setting configuration values to the specific file format configurations,
    /// based on the current format selected. If no format is selected, it returns an error.
    ///
    /// # Parameters
    ///
    /// * `key`: The configuration key specifying which setting to adjust, prefixed with the format (e.g., "format.delimiter")
    ///   for CSV format.
    /// * `value`: The value to set for the specified configuration key.
    ///
    /// # Returns
    ///
    /// A result indicating success or an error if the key is not recognized, if a format is not specified,
    /// or if setting the configuration value fails for the specific format.
    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        // Extensions are handled in the public `ConfigOptions::set`
        let (key, rem) = key.split_once('.').unwrap_or((key, ""));
        match key {
            "format" => {
                let Some(format) = &self.current_format else {
                    return _config_err!("Specify a format for TableOptions");
                };
                match format {
                    #[cfg(feature = "parquet")]
                    ConfigFileType::PARQUET => self.parquet.set(rem, value),
                    ConfigFileType::CSV => self.csv.set(rem, value),
                    ConfigFileType::JSON => self.json.set(rem, value),
                }
            }
            _ => _config_err!("Config value \"{key}\" not found on TableOptions"),
        }
    }
}

impl TableOptions {
    /// Constructs a new instance of `TableOptions` with default settings.
    ///
    /// # Returns
    ///
    /// A new `TableOptions` instance with default configuration values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new `TableOptions` instance initialized with settings from a given session config.
    ///
    /// # Parameters
    ///
    /// * `config`: A reference to the session `ConfigOptions` from which to derive initial settings.
    ///
    /// # Returns
    ///
    /// A new `TableOptions` instance with settings applied from the session config.
    pub fn default_from_session_config(config: &ConfigOptions) -> Self {
        let initial = TableOptions::default();
        initial.combine_with_session_config(config)
    }

    /// Updates the current `TableOptions` with settings from a given session config.
    ///
    /// # Parameters
    ///
    /// * `config`: A reference to the session `ConfigOptions` whose settings are to be applied.
    ///
    /// # Returns
    ///
    /// A new `TableOptions` instance with updated settings from the session config.
    #[must_use = "this method returns a new instance"]
    pub fn combine_with_session_config(&self, config: &ConfigOptions) -> Self {
        let mut clone = self.clone();
        clone.parquet.global = config.execution.parquet.clone();
        clone
    }

    /// Sets the file format for the table.
    ///
    /// # Parameters
    ///
    /// * `format`: The file format to use (e.g., CSV, Parquet).
    pub fn set_config_format(&mut self, format: ConfigFileType) {
        self.current_format = Some(format);
    }

    /// Sets the extensions for this `TableOptions` instance.
    ///
    /// # Parameters
    ///
    /// * `extensions`: The `Extensions` instance to set.
    ///
    /// # Returns
    ///
    /// A new `TableOptions` instance with the specified extensions applied.
    pub fn with_extensions(mut self, extensions: Extensions) -> Self {
        self.extensions = extensions;
        self
    }

    /// Sets a specific configuration option.
    ///
    /// # Parameters
    ///
    /// * `key`: The configuration key (e.g., "format.delimiter").
    /// * `value`: The value to set for the specified key.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure in setting the configuration option.
    pub fn set(&mut self, key: &str, value: &str) -> Result<()> {
        let Some((prefix, _)) = key.split_once('.') else {
            return _config_err!("could not find config namespace for key \"{key}\"");
        };

        if prefix == "format" {
            return ConfigField::set(self, key, value);
        }

        if prefix == "execution" {
            return Ok(());
        }

        let Some(e) = self.extensions.0.get_mut(prefix) else {
            return _config_err!("Could not find config namespace \"{prefix}\"");
        };
        e.0.set(key, value)
    }

    /// Initializes a new `TableOptions` from a hash map of string settings.
    ///
    /// # Parameters
    ///
    /// * `settings`: A hash map where each key-value pair represents a configuration setting.
    ///
    /// # Returns
    ///
    /// A result containing the new `TableOptions` instance or an error if any setting could not be applied.
    pub fn from_string_hash_map(settings: &HashMap<String, String>) -> Result<Self> {
        let mut ret = Self::default();
        for (k, v) in settings {
            ret.set(k, v)?;
        }

        Ok(ret)
    }

    /// Modifies the current `TableOptions` instance with settings from a hash map.
    ///
    /// # Parameters
    ///
    /// * `settings`: A hash map where each key-value pair represents a configuration setting.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure in applying the settings.
    pub fn alter_with_string_hash_map(
        &mut self,
        settings: &HashMap<String, String>,
    ) -> Result<()> {
        for (k, v) in settings {
            self.set(k, v)?;
        }
        Ok(())
    }

    /// Retrieves all configuration entries from this `TableOptions`.
    ///
    /// # Returns
    ///
    /// A vector of `ConfigEntry` instances, representing all the configuration options within this `TableOptions`.
    pub fn entries(&self) -> Vec<ConfigEntry> {
        struct Visitor(Vec<ConfigEntry>);

        impl Visit for Visitor {
            fn some<V: Display>(
                &mut self,
                key: &str,
                value: V,
                description: &'static str,
            ) {
                self.0.push(ConfigEntry {
                    key: key.to_string(),
                    value: Some(value.to_string()),
                    description,
                })
            }

            fn none(&mut self, key: &str, description: &'static str) {
                self.0.push(ConfigEntry {
                    key: key.to_string(),
                    value: None,
                    description,
                })
            }
        }

        let mut v = Visitor(vec![]);
        self.visit(&mut v, "format", "");

        v.0.extend(self.extensions.0.values().flat_map(|e| e.0.entries()));
        v.0
    }
}

/// Options that control how Parquet files are read, including global options
/// that apply to all columns and optional column-specific overrides
///
/// Closely tied to [`ParquetWriterOptions`](crate::file_options::parquet_writer::ParquetWriterOptions).
/// Properties not included in [`TableParquetOptions`] may not be configurable at the external API
/// (e.g. sorting_columns).
#[derive(Clone, Default, Debug, PartialEq)]
pub struct TableParquetOptions {
    /// Global Parquet options that propagates to all columns.
    pub global: ParquetOptions,
    /// Column specific options. Default usage is parquet.XX::column.
    pub column_specific_options: HashMap<String, ParquetColumnOptions>,
    /// Additional file-level metadata to include. Inserted into the key_value_metadata
    /// for the written [`FileMetaData`](https://docs.rs/parquet/latest/parquet/file/metadata/struct.FileMetaData.html).
    ///
    /// Multiple entries are permitted
    /// ```sql
    /// OPTIONS (
    ///    'format.metadata::key1' '',
    ///    'format.metadata::key2' 'value',
    ///    'format.metadata::key3' 'value has spaces',
    ///    'format.metadata::key4' 'value has special chars :: :',
    ///    'format.metadata::key_dupe' 'original will be overwritten',
    ///    'format.metadata::key_dupe' 'final'
    /// )
    /// ```
    pub key_value_metadata: HashMap<String, Option<String>>,
    /// Options for configuring Parquet modular encryption
    ///
    /// To use Parquet encryption, you must enable the `parquet_encryption` feature flag, as it is not activated by default.
    /// See ConfigFileEncryptionProperties and ConfigFileDecryptionProperties in datafusion/common/src/config.rs
    /// These can be set via 'format.crypto', for example:
    /// ```sql
    /// OPTIONS (
    ///    'format.crypto.file_encryption.encrypt_footer' 'true',
    ///    'format.crypto.file_encryption.footer_key_as_hex' '30313233343536373839303132333435',  -- b"0123456789012345" */
    ///    'format.crypto.file_encryption.column_key_as_hex::double_field' '31323334353637383930313233343530', -- b"1234567890123450"
    ///    'format.crypto.file_encryption.column_key_as_hex::float_field' '31323334353637383930313233343531', -- b"1234567890123451"
    ///     -- Same for decryption
    ///    'format.crypto.file_decryption.footer_key_as_hex' '30313233343536373839303132333435', -- b"0123456789012345"
    ///    'format.crypto.file_decryption.column_key_as_hex::double_field' '31323334353637383930313233343530', -- b"1234567890123450"
    ///    'format.crypto.file_decryption.column_key_as_hex::float_field' '31323334353637383930313233343531', -- b"1234567890123451"
    /// )
    /// ```
    /// See datafusion-cli/tests/sql/encrypted_parquet.sql for a more complete example.
    /// Note that keys must be provided as in hex format since these are binary strings.
    pub crypto: ParquetEncryptionOptions,
}

impl TableParquetOptions {
    /// Return new default TableParquetOptions
    pub fn new() -> Self {
        Self::default()
    }

    /// Set whether the encoding of the arrow metadata should occur
    /// during the writing of parquet.
    ///
    /// Default is to encode the arrow schema in the file kv_metadata.
    pub fn with_skip_arrow_metadata(self, skip: bool) -> Self {
        Self {
            global: ParquetOptions {
                skip_arrow_metadata: skip,
                ..self.global
            },
            ..self
        }
    }

    /// Retrieves all configuration entries from this `TableParquetOptions`.
    ///
    /// # Returns
    ///
    /// A vector of `ConfigEntry` instances, representing all the configuration options within this
    pub fn entries(self: &TableParquetOptions) -> Vec<ConfigEntry> {
        struct Visitor(Vec<ConfigEntry>);

        impl Visit for Visitor {
            fn some<V: Display>(
                &mut self,
                key: &str,
                value: V,
                description: &'static str,
            ) {
                self.0.push(ConfigEntry {
                    key: key[1..].to_string(),
                    value: Some(value.to_string()),
                    description,
                })
            }

            fn none(&mut self, key: &str, description: &'static str) {
                self.0.push(ConfigEntry {
                    key: key[1..].to_string(),
                    value: None,
                    description,
                })
            }
        }

        let mut v = Visitor(vec![]);
        self.visit(&mut v, "", "");

        v.0
    }
}

impl ConfigField for TableParquetOptions {
    fn visit<V: Visit>(&self, v: &mut V, key_prefix: &str, description: &'static str) {
        self.global.visit(v, key_prefix, description);
        self.column_specific_options
            .visit(v, key_prefix, description);
        self.crypto
            .visit(v, &format!("{key_prefix}.crypto"), description);
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        // Determine if the key is a global, metadata, or column-specific setting
        if key.starts_with("metadata::") {
            let k = match key.split("::").collect::<Vec<_>>()[..] {
                [_meta] | [_meta, ""] => {
                    return _config_err!(
                        "Invalid metadata key provided, missing key in metadata::<key>"
                    );
                }
                [_meta, k] => k.into(),
                _ => {
                    return _config_err!(
                        "Invalid metadata key provided, found too many '::' in \"{key}\""
                    );
                }
            };
            self.key_value_metadata.insert(k, Some(value.into()));
            Ok(())
        } else if let Some(crypto_feature) = key.strip_prefix("crypto.") {
            self.crypto.set(crypto_feature, value)
        } else if key.contains("::") {
            self.column_specific_options.set(key, value)
        } else {
            self.global.set(key, value)
        }
    }
}

macro_rules! config_namespace_with_hashmap {
    (
     $(#[doc = $struct_d:tt])*
     $(#[deprecated($($struct_depr:tt)*)])?  // Optional struct-level deprecated attribute
     $vis:vis struct $struct_name:ident {
        $(
        $(#[doc = $d:tt])*
        $(#[deprecated($($field_depr:tt)*)])? // Optional field-level deprecated attribute
        $field_vis:vis $field_name:ident : $field_type:ty, $(transform = $transform:expr,)? default = $default:expr
        )*$(,)*
    }
    ) => {

        $(#[doc = $struct_d])*
        $(#[deprecated($($struct_depr)*)])?  // Apply struct deprecation
        #[derive(Debug, Clone, PartialEq)]
        $vis struct $struct_name{
            $(
            $(#[doc = $d])*
            $(#[deprecated($($field_depr)*)])? // Apply field deprecation
            $field_vis $field_name : $field_type,
            )*
        }

        impl ConfigField for $struct_name {
            fn set(&mut self, key: &str, value: &str) -> Result<()> {
                let (key, rem) = key.split_once('.').unwrap_or((key, ""));
                match key {
                    $(
                       stringify!($field_name) => {
                           // Handle deprecated fields
                           #[allow(deprecated)] // Allow deprecated fields
                           $(let value = $transform(value);)?
                           self.$field_name.set(rem, value.as_ref())
                       },
                    )*
                    _ => _config_err!(
                        "Config value \"{}\" not found on {}", key, stringify!($struct_name)
                    )
                }
            }

            fn visit<V: Visit>(&self, v: &mut V, key_prefix: &str, _description: &'static str) {
                $(
                let key = format!(concat!("{}.", stringify!($field_name)), key_prefix);
                let desc = concat!($($d),*).trim();
                // Handle deprecated fields
                #[allow(deprecated)]
                self.$field_name.visit(v, key.as_str(), desc);
                )*
            }
        }

        impl Default for $struct_name {
            fn default() -> Self {
                #[allow(deprecated)]
                Self {
                    $($field_name: $default),*
                }
            }
        }

        impl ConfigField for HashMap<String,$struct_name> {
            fn set(&mut self, key: &str, value: &str) -> Result<()> {
                let parts: Vec<&str> = key.splitn(2, "::").collect();
                match parts.as_slice() {
                    [inner_key, hashmap_key] => {
                        // Get or create the struct for the specified key
                        let inner_value = self
                            .entry((*hashmap_key).to_owned())
                            .or_insert_with($struct_name::default);

                        inner_value.set(inner_key, value)
                    }
                    _ => _config_err!("Unrecognized key '{key}'."),
                }
            }

            fn visit<V: Visit>(&self, v: &mut V, key_prefix: &str, _description: &'static str) {
                for (column_name, col_options) in self {
                    $(
                    let key = format!("{}.{field}::{}", key_prefix, column_name, field = stringify!($field_name));
                    let desc = concat!($($d),*).trim();
                    #[allow(deprecated)]
                    col_options.$field_name.visit(v, key.as_str(), desc);
                    )*
                }
            }
        }
    }
}

config_namespace_with_hashmap! {
    /// Options controlling parquet format for individual columns.
    ///
    /// See [`ParquetOptions`] for more details
    pub struct ParquetColumnOptions {
        /// Sets if bloom filter is enabled for the column path.
        pub bloom_filter_enabled: Option<bool>, default = None

        /// Sets encoding for the column path.
        /// Valid values are: plain, plain_dictionary, rle,
        /// bit_packed, delta_binary_packed, delta_length_byte_array,
        /// delta_byte_array, rle_dictionary, and byte_stream_split.
        /// These values are not case-sensitive. If NULL, uses
        /// default parquet options
        pub encoding: Option<String>, default = None

        /// Sets if dictionary encoding is enabled for the column path. If NULL, uses
        /// default parquet options
        pub dictionary_enabled: Option<bool>, default = None

        /// Sets default parquet compression codec for the column path.
        /// Valid values are: uncompressed, snappy, gzip(level),
        /// lzo, brotli(level), lz4, zstd(level), and lz4_raw.
        /// These values are not case-sensitive. If NULL, uses
        /// default parquet options
        pub compression: Option<String>, transform = str::to_lowercase, default = None

        /// Sets if statistics are enabled for the column
        /// Valid values are: "none", "chunk", and "page"
        /// These values are not case sensitive. If NULL, uses
        /// default parquet options
        pub statistics_enabled: Option<String>, default = None

        /// Sets bloom filter false positive probability for the column path. If NULL, uses
        /// default parquet options
        pub bloom_filter_fpp: Option<f64>, default = None

        /// Sets bloom filter number of distinct values. If NULL, uses
        /// default parquet options
        pub bloom_filter_ndv: Option<u64>, default = None
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConfigFileEncryptionProperties {
    /// Should the parquet footer be encrypted
    /// default is true
    pub encrypt_footer: bool,
    /// Key to use for the parquet footer encoded in hex format
    pub footer_key_as_hex: String,
    /// Metadata information for footer key
    pub footer_key_metadata_as_hex: String,
    /// HashMap of column names --> (key in hex format, metadata)
    pub column_encryption_properties: HashMap<String, ColumnEncryptionProperties>,
    /// AAD prefix string uniquely identifies the file and prevents file swapping
    pub aad_prefix_as_hex: String,
    /// If true, store the AAD prefix in the file
    /// default is false
    pub store_aad_prefix: bool,
}

// Setup to match EncryptionPropertiesBuilder::new()
impl Default for ConfigFileEncryptionProperties {
    fn default() -> Self {
        ConfigFileEncryptionProperties {
            encrypt_footer: true,
            footer_key_as_hex: String::new(),
            footer_key_metadata_as_hex: String::new(),
            column_encryption_properties: Default::default(),
            aad_prefix_as_hex: String::new(),
            store_aad_prefix: false,
        }
    }
}

config_namespace_with_hashmap! {
    pub struct ColumnEncryptionProperties {
        /// Per column encryption key
        pub column_key_as_hex: String, default = "".to_string()
        /// Per column encryption key metadata
        pub column_metadata_as_hex: Option<String>, default = None
    }
}

impl ConfigField for ConfigFileEncryptionProperties {
    fn visit<V: Visit>(&self, v: &mut V, key_prefix: &str, _description: &'static str) {
        let key = format!("{key_prefix}.encrypt_footer");
        let desc = "Encrypt the footer";
        self.encrypt_footer.visit(v, key.as_str(), desc);

        let key = format!("{key_prefix}.footer_key_as_hex");
        let desc = "Key to use for the parquet footer";
        self.footer_key_as_hex.visit(v, key.as_str(), desc);

        let key = format!("{key_prefix}.footer_key_metadata_as_hex");
        let desc = "Metadata to use for the parquet footer";
        self.footer_key_metadata_as_hex.visit(v, key.as_str(), desc);

        self.column_encryption_properties.visit(v, key_prefix, desc);

        let key = format!("{key_prefix}.aad_prefix_as_hex");
        let desc = "AAD prefix to use";
        self.aad_prefix_as_hex.visit(v, key.as_str(), desc);

        let key = format!("{key_prefix}.store_aad_prefix");
        let desc = "If true, store the AAD prefix";
        self.store_aad_prefix.visit(v, key.as_str(), desc);

        self.aad_prefix_as_hex.visit(v, key.as_str(), desc);
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        // Any hex encoded values must be pre-encoded using
        // hex::encode() before calling set.

        if key.contains("::") {
            // Handle any column specific properties
            return self.column_encryption_properties.set(key, value);
        };

        let (key, rem) = key.split_once('.').unwrap_or((key, ""));
        match key {
            "encrypt_footer" => self.encrypt_footer.set(rem, value.as_ref()),
            "footer_key_as_hex" => self.footer_key_as_hex.set(rem, value.as_ref()),
            "footer_key_metadata_as_hex" => {
                self.footer_key_metadata_as_hex.set(rem, value.as_ref())
            }
            "aad_prefix_as_hex" => self.aad_prefix_as_hex.set(rem, value.as_ref()),
            "store_aad_prefix" => self.store_aad_prefix.set(rem, value.as_ref()),
            _ => _config_err!(
                "Config value \"{}\" not found on ConfigFileEncryptionProperties",
                key
            ),
        }
    }
}

#[cfg(feature = "parquet_encryption")]
impl From<ConfigFileEncryptionProperties> for FileEncryptionProperties {
    fn from(val: ConfigFileEncryptionProperties) -> Self {
        let mut fep = FileEncryptionProperties::builder(
            hex::decode(val.footer_key_as_hex).unwrap(),
        )
        .with_plaintext_footer(!val.encrypt_footer)
        .with_aad_prefix_storage(val.store_aad_prefix);

        if !val.footer_key_metadata_as_hex.is_empty() {
            fep = fep.with_footer_key_metadata(
                hex::decode(&val.footer_key_metadata_as_hex)
                    .expect("Invalid footer key metadata"),
            );
        }

        for (column_name, encryption_props) in val.column_encryption_properties.iter() {
            let encryption_key = hex::decode(&encryption_props.column_key_as_hex)
                .expect("Invalid column encryption key");
            let key_metadata = encryption_props
                .column_metadata_as_hex
                .as_ref()
                .map(|x| hex::decode(x).expect("Invalid column metadata"));
            match key_metadata {
                Some(key_metadata) => {
                    fep = fep.with_column_key_and_metadata(
                        column_name,
                        encryption_key,
                        key_metadata,
                    );
                }
                None => {
                    fep = fep.with_column_key(column_name, encryption_key);
                }
            }
        }

        if !val.aad_prefix_as_hex.is_empty() {
            let aad_prefix: Vec<u8> =
                hex::decode(&val.aad_prefix_as_hex).expect("Invalid AAD prefix");
            fep = fep.with_aad_prefix(aad_prefix);
        }
        Arc::unwrap_or_clone(fep.build().unwrap())
    }
}

#[cfg(feature = "parquet_encryption")]
impl From<&Arc<FileEncryptionProperties>> for ConfigFileEncryptionProperties {
    fn from(f: &Arc<FileEncryptionProperties>) -> Self {
        let (column_names_vec, column_keys_vec, column_metas_vec) = f.column_keys();

        let mut column_encryption_properties: HashMap<
            String,
            ColumnEncryptionProperties,
        > = HashMap::new();

        for (i, column_name) in column_names_vec.iter().enumerate() {
            let column_key_as_hex = hex::encode(&column_keys_vec[i]);
            let column_metadata_as_hex: Option<String> =
                column_metas_vec.get(i).map(hex::encode);
            column_encryption_properties.insert(
                column_name.clone(),
                ColumnEncryptionProperties {
                    column_key_as_hex,
                    column_metadata_as_hex,
                },
            );
        }
        let mut aad_prefix: Vec<u8> = Vec::new();
        if let Some(prefix) = f.aad_prefix() {
            aad_prefix = prefix.clone();
        }
        ConfigFileEncryptionProperties {
            encrypt_footer: f.encrypt_footer(),
            footer_key_as_hex: hex::encode(f.footer_key()),
            footer_key_metadata_as_hex: f
                .footer_key_metadata()
                .map(hex::encode)
                .unwrap_or_default(),
            column_encryption_properties,
            aad_prefix_as_hex: hex::encode(aad_prefix),
            store_aad_prefix: f.store_aad_prefix(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConfigFileDecryptionProperties {
    /// Binary string to use for the parquet footer encoded in hex format
    pub footer_key_as_hex: String,
    /// HashMap of column names --> key in hex format
    pub column_decryption_properties: HashMap<String, ColumnDecryptionProperties>,
    /// AAD prefix string uniquely identifies the file and prevents file swapping
    pub aad_prefix_as_hex: String,
    /// If true, then verify signature for files with plaintext footers.
    /// default = true
    pub footer_signature_verification: bool,
}

config_namespace_with_hashmap! {
    pub struct ColumnDecryptionProperties {
        /// Per column encryption key
        pub column_key_as_hex: String, default = "".to_string()
    }
}

// Setup to match DecryptionPropertiesBuilder::new()
impl Default for ConfigFileDecryptionProperties {
    fn default() -> Self {
        ConfigFileDecryptionProperties {
            footer_key_as_hex: String::new(),
            column_decryption_properties: Default::default(),
            aad_prefix_as_hex: String::new(),
            footer_signature_verification: true,
        }
    }
}

impl ConfigField for ConfigFileDecryptionProperties {
    fn visit<V: Visit>(&self, v: &mut V, key_prefix: &str, _description: &'static str) {
        let key = format!("{key_prefix}.footer_key_as_hex");
        let desc = "Key to use for the parquet footer";
        self.footer_key_as_hex.visit(v, key.as_str(), desc);

        let key = format!("{key_prefix}.aad_prefix_as_hex");
        let desc = "AAD prefix to use";
        self.aad_prefix_as_hex.visit(v, key.as_str(), desc);

        let key = format!("{key_prefix}.footer_signature_verification");
        let desc = "If true, verify the footer signature";
        self.footer_signature_verification
            .visit(v, key.as_str(), desc);

        self.column_decryption_properties.visit(v, key_prefix, desc);
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        // Any hex encoded values must be pre-encoded using
        // hex::encode() before calling set.

        if key.contains("::") {
            // Handle any column specific properties
            return self.column_decryption_properties.set(key, value);
        };

        let (key, rem) = key.split_once('.').unwrap_or((key, ""));
        match key {
            "footer_key_as_hex" => self.footer_key_as_hex.set(rem, value.as_ref()),
            "aad_prefix_as_hex" => self.aad_prefix_as_hex.set(rem, value.as_ref()),
            "footer_signature_verification" => {
                self.footer_signature_verification.set(rem, value.as_ref())
            }
            _ => _config_err!(
                "Config value \"{}\" not found on ConfigFileDecryptionProperties",
                key
            ),
        }
    }
}

#[cfg(feature = "parquet_encryption")]
impl From<ConfigFileDecryptionProperties> for FileDecryptionProperties {
    fn from(val: ConfigFileDecryptionProperties) -> Self {
        let mut column_names: Vec<&str> = Vec::new();
        let mut column_keys: Vec<Vec<u8>> = Vec::new();

        for (col_name, decryption_properties) in val.column_decryption_properties.iter() {
            column_names.push(col_name.as_str());
            column_keys.push(
                hex::decode(&decryption_properties.column_key_as_hex)
                    .expect("Invalid column decryption key"),
            );
        }

        let mut fep = FileDecryptionProperties::builder(
            hex::decode(val.footer_key_as_hex).expect("Invalid footer key"),
        )
        .with_column_keys(column_names, column_keys)
        .unwrap();

        if !val.footer_signature_verification {
            fep = fep.disable_footer_signature_verification();
        }

        if !val.aad_prefix_as_hex.is_empty() {
            let aad_prefix =
                hex::decode(&val.aad_prefix_as_hex).expect("Invalid AAD prefix");
            fep = fep.with_aad_prefix(aad_prefix);
        }

        Arc::unwrap_or_clone(fep.build().unwrap())
    }
}

#[cfg(feature = "parquet_encryption")]
impl From<&Arc<FileDecryptionProperties>> for ConfigFileDecryptionProperties {
    fn from(f: &Arc<FileDecryptionProperties>) -> Self {
        let (column_names_vec, column_keys_vec) = f.column_keys();
        let mut column_decryption_properties: HashMap<
            String,
            ColumnDecryptionProperties,
        > = HashMap::new();
        for (i, column_name) in column_names_vec.iter().enumerate() {
            let props = ColumnDecryptionProperties {
                column_key_as_hex: hex::encode(column_keys_vec[i].clone()),
            };
            column_decryption_properties.insert(column_name.clone(), props);
        }

        let mut aad_prefix: Vec<u8> = Vec::new();
        if let Some(prefix) = f.aad_prefix() {
            aad_prefix = prefix.clone();
        }
        ConfigFileDecryptionProperties {
            footer_key_as_hex: hex::encode(
                f.footer_key(None).unwrap_or_default().as_ref(),
            ),
            column_decryption_properties,
            aad_prefix_as_hex: hex::encode(aad_prefix),
            footer_signature_verification: f.check_plaintext_footer_integrity(),
        }
    }
}

/// Holds implementation-specific options for an encryption factory
#[derive(Clone, Debug, Default, PartialEq)]
pub struct EncryptionFactoryOptions {
    pub options: HashMap<String, String>,
}

impl ConfigField for EncryptionFactoryOptions {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, _description: &'static str) {
        for (option_key, option_value) in &self.options {
            v.some(
                &format!("{key}.{option_key}"),
                option_value,
                "Encryption factory specific option",
            );
        }
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        self.options.insert(key.to_owned(), value.to_owned());
        Ok(())
    }
}

impl EncryptionFactoryOptions {
    /// Convert these encryption factory options to an [`ExtensionOptions`] instance.
    pub fn to_extension_options<T: ExtensionOptions + Default>(&self) -> Result<T> {
        let mut options = T::default();
        for (key, value) in &self.options {
            options.set(key, value)?;
        }
        Ok(options)
    }
}

config_namespace! {
    /// Options controlling CSV format
    pub struct CsvOptions {
        /// Specifies whether there is a CSV header (i.e. the first line
        /// consists of is column names). The value `None` indicates that
        /// the configuration should be consulted.
        pub has_header: Option<bool>, default = None
        pub delimiter: u8, default = b','
        pub quote: u8, default = b'"'
        pub terminator: Option<u8>, default = None
        pub escape: Option<u8>, default = None
        pub double_quote: Option<bool>, default = None
        /// Specifies whether newlines in (quoted) values are supported.
        ///
        /// Parsing newlines in quoted values may be affected by execution behaviour such as
        /// parallel file scanning. Setting this to `true` ensures that newlines in values are
        /// parsed successfully, which may reduce performance.
        ///
        /// The default behaviour depends on the `datafusion.catalog.newlines_in_values` setting.
        pub newlines_in_values: Option<bool>, default = None
        pub compression: CompressionTypeVariant, default = CompressionTypeVariant::UNCOMPRESSED
        pub schema_infer_max_rec: Option<usize>, default = None
        pub date_format: Option<String>, default = None
        pub datetime_format: Option<String>, default = None
        pub timestamp_format: Option<String>, default = None
        pub timestamp_tz_format: Option<String>, default = None
        pub time_format: Option<String>, default = None
        // The output format for Nulls in the CSV writer.
        pub null_value: Option<String>, default = None
        // The input regex for Nulls when loading CSVs.
        pub null_regex: Option<String>, default = None
        pub comment: Option<u8>, default = None
        /// Whether to allow truncated rows when parsing, both within a single file and across files.
        ///
        /// When set to false (default), reading a single CSV file which has rows of different lengths will
        /// error; if reading multiple CSV files with different number of columns, it will also fail.
        ///
        /// When set to true, reading a single CSV file with rows of different lengths will pad the truncated
        /// rows with null values for the missing columns; if reading multiple CSV files with different number
        /// of columns, it creates a union schema containing all columns found across the files, and will
        /// pad any files missing columns with null values for their rows.
        pub truncated_rows: Option<bool>, default = None
    }
}

impl CsvOptions {
    /// Set a limit in terms of records to scan to infer the schema
    /// - default to `DEFAULT_SCHEMA_INFER_MAX_RECORD`
    pub fn with_compression(
        mut self,
        compression_type_variant: CompressionTypeVariant,
    ) -> Self {
        self.compression = compression_type_variant;
        self
    }

    /// Set a limit in terms of records to scan to infer the schema
    /// - default to `DEFAULT_SCHEMA_INFER_MAX_RECORD`
    pub fn with_schema_infer_max_rec(mut self, max_rec: usize) -> Self {
        self.schema_infer_max_rec = Some(max_rec);
        self
    }

    /// Set true to indicate that the first line is a header.
    /// - default to true
    pub fn with_has_header(mut self, has_header: bool) -> Self {
        self.has_header = Some(has_header);
        self
    }

    /// Returns true if the first line is a header. If format options does not
    /// specify whether there is a header, returns `None` (indicating that the
    /// configuration should be consulted).
    pub fn has_header(&self) -> Option<bool> {
        self.has_header
    }

    /// The character separating values within a row.
    /// - default to ','
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    /// The quote character in a row.
    /// - default to '"'
    pub fn with_quote(mut self, quote: u8) -> Self {
        self.quote = quote;
        self
    }

    /// The character that terminates a row.
    /// - default to None (CRLF)
    pub fn with_terminator(mut self, terminator: Option<u8>) -> Self {
        self.terminator = terminator;
        self
    }

    /// The escape character in a row.
    /// - default is None
    pub fn with_escape(mut self, escape: Option<u8>) -> Self {
        self.escape = escape;
        self
    }

    /// Set true to indicate that the CSV quotes should be doubled.
    /// - default to true
    pub fn with_double_quote(mut self, double_quote: bool) -> Self {
        self.double_quote = Some(double_quote);
        self
    }

    /// Specifies whether newlines in (quoted) values are supported.
    ///
    /// Parsing newlines in quoted values may be affected by execution behaviour such as
    /// parallel file scanning. Setting this to `true` ensures that newlines in values are
    /// parsed successfully, which may reduce performance.
    ///
    /// The default behaviour depends on the `datafusion.catalog.newlines_in_values` setting.
    pub fn with_newlines_in_values(mut self, newlines_in_values: bool) -> Self {
        self.newlines_in_values = Some(newlines_in_values);
        self
    }

    /// Set a `CompressionTypeVariant` of CSV
    /// - defaults to `CompressionTypeVariant::UNCOMPRESSED`
    pub fn with_file_compression_type(
        mut self,
        compression: CompressionTypeVariant,
    ) -> Self {
        self.compression = compression;
        self
    }

    /// Whether to allow truncated rows when parsing.
    /// By default this is set to false and will error if the CSV rows have different lengths.
    /// When set to true then it will allow records with less than the expected number of columns and fill the missing columns with nulls.
    /// If the record’s schema is not nullable, then it will still return an error.
    pub fn with_truncated_rows(mut self, allow: bool) -> Self {
        self.truncated_rows = Some(allow);
        self
    }

    /// The delimiter character.
    pub fn delimiter(&self) -> u8 {
        self.delimiter
    }

    /// The quote character.
    pub fn quote(&self) -> u8 {
        self.quote
    }

    /// The terminator character.
    pub fn terminator(&self) -> Option<u8> {
        self.terminator
    }

    /// The escape character.
    pub fn escape(&self) -> Option<u8> {
        self.escape
    }
}

config_namespace! {
    /// Options controlling JSON format
    pub struct JsonOptions {
        pub compression: CompressionTypeVariant, default = CompressionTypeVariant::UNCOMPRESSED
        pub schema_infer_max_rec: Option<usize>, default = None
    }
}

pub trait OutputFormatExt: Display {}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum OutputFormat {
    CSV(CsvOptions),
    JSON(JsonOptions),
    #[cfg(feature = "parquet")]
    PARQUET(TableParquetOptions),
    AVRO,
    ARROW,
}

impl Display for OutputFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let out = match self {
            OutputFormat::CSV(_) => "csv",
            OutputFormat::JSON(_) => "json",
            #[cfg(feature = "parquet")]
            OutputFormat::PARQUET(_) => "parquet",
            OutputFormat::AVRO => "avro",
            OutputFormat::ARROW => "arrow",
        };
        write!(f, "{out}")
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "parquet")]
    use crate::config::TableParquetOptions;
    use crate::config::{
        ConfigEntry, ConfigExtension, ConfigField, ConfigFileType, ExtensionOptions,
        Extensions, TableOptions,
    };
    use std::any::Any;
    use std::collections::HashMap;

    #[derive(Default, Debug, Clone)]
    pub struct TestExtensionConfig {
        /// Should "foo" be replaced by "bar"?
        pub properties: HashMap<String, String>,
    }

    impl ExtensionOptions for TestExtensionConfig {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn cloned(&self) -> Box<dyn ExtensionOptions> {
            Box::new(self.clone())
        }

        fn set(&mut self, key: &str, value: &str) -> crate::Result<()> {
            let (key, rem) = key.split_once('.').unwrap_or((key, ""));
            assert_eq!(key, "test");
            self.properties.insert(rem.to_owned(), value.to_owned());
            Ok(())
        }

        fn entries(&self) -> Vec<ConfigEntry> {
            self.properties
                .iter()
                .map(|(k, v)| ConfigEntry {
                    key: k.into(),
                    value: Some(v.into()),
                    description: "",
                })
                .collect()
        }
    }

    impl ConfigExtension for TestExtensionConfig {
        const PREFIX: &'static str = "test";
    }

    #[test]
    fn create_table_config() {
        let mut extension = Extensions::new();
        extension.insert(TestExtensionConfig::default());
        let table_config = TableOptions::new().with_extensions(extension);
        let kafka_config = table_config.extensions.get::<TestExtensionConfig>();
        assert!(kafka_config.is_some())
    }

    #[test]
    fn alter_test_extension_config() {
        let mut extension = Extensions::new();
        extension.insert(TestExtensionConfig::default());
        let mut table_config = TableOptions::new().with_extensions(extension);
        table_config.set_config_format(ConfigFileType::CSV);
        table_config.set("format.delimiter", ";").unwrap();
        assert_eq!(table_config.csv.delimiter, b';');
        table_config.set("test.bootstrap.servers", "asd").unwrap();
        let kafka_config = table_config
            .extensions
            .get::<TestExtensionConfig>()
            .unwrap();
        assert_eq!(
            kafka_config.properties.get("bootstrap.servers").unwrap(),
            "asd"
        );
    }

    #[test]
    fn iter_test_extension_config() {
        let mut extension = Extensions::new();
        extension.insert(TestExtensionConfig::default());
        let table_config = TableOptions::new().with_extensions(extension);
        let extensions = table_config.extensions.iter().collect::<Vec<_>>();
        assert_eq!(extensions.len(), 1);
        assert_eq!(extensions[0].0, TestExtensionConfig::PREFIX);
    }

    #[test]
    fn csv_u8_table_options() {
        let mut table_config = TableOptions::new();
        table_config.set_config_format(ConfigFileType::CSV);
        table_config.set("format.delimiter", ";").unwrap();
        assert_eq!(table_config.csv.delimiter as char, ';');
        table_config.set("format.escape", "\"").unwrap();
        assert_eq!(table_config.csv.escape.unwrap() as char, '"');
        table_config.set("format.escape", "\'").unwrap();
        assert_eq!(table_config.csv.escape.unwrap() as char, '\'');
    }

    #[test]
    fn warning_only_not_default() {
        use std::sync::atomic::AtomicUsize;
        static COUNT: AtomicUsize = AtomicUsize::new(0);
        use log::{Level, LevelFilter, Metadata, Record};
        struct SimpleLogger;
        impl log::Log for SimpleLogger {
            fn enabled(&self, metadata: &Metadata) -> bool {
                metadata.level() <= Level::Info
            }

            fn log(&self, record: &Record) {
                if self.enabled(record.metadata()) {
                    COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
            fn flush(&self) {}
        }
        log::set_logger(&SimpleLogger).unwrap();
        log::set_max_level(LevelFilter::Info);
        let mut sql_parser_options = crate::config::SqlParserOptions::default();
        sql_parser_options
            .set("enable_options_value_normalization", "false")
            .unwrap();
        assert_eq!(COUNT.load(std::sync::atomic::Ordering::Relaxed), 0);
        sql_parser_options
            .set("enable_options_value_normalization", "true")
            .unwrap();
        assert_eq!(COUNT.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[test]
    fn reset_nested_scalar_reports_helpful_error() {
        let mut value = true;
        let err = <bool as ConfigField>::reset(&mut value, "nested").unwrap_err();
        let message = err.to_string();
        assert!(
            message.starts_with(
                "Invalid or Unsupported Configuration: Config field is a scalar bool and does not have nested field \"nested\""
            ),
            "unexpected error message: {message}"
        );
    }

    #[cfg(feature = "parquet")]
    #[test]
    fn parquet_table_options() {
        let mut table_config = TableOptions::new();
        table_config.set_config_format(ConfigFileType::PARQUET);
        table_config
            .set("format.bloom_filter_enabled::col1", "true")
            .unwrap();
        assert_eq!(
            table_config.parquet.column_specific_options["col1"].bloom_filter_enabled,
            Some(true)
        );
    }

    #[cfg(feature = "parquet_encryption")]
    #[test]
    fn parquet_table_encryption() {
        use crate::config::{
            ConfigFileDecryptionProperties, ConfigFileEncryptionProperties,
        };
        use parquet::encryption::decrypt::FileDecryptionProperties;
        use parquet::encryption::encrypt::FileEncryptionProperties;
        use std::sync::Arc;

        let footer_key = b"0123456789012345".to_vec(); // 128bit/16
        let column_names = vec!["double_field", "float_field"];
        let column_keys =
            vec![b"1234567890123450".to_vec(), b"1234567890123451".to_vec()];

        let file_encryption_properties =
            FileEncryptionProperties::builder(footer_key.clone())
                .with_column_keys(column_names.clone(), column_keys.clone())
                .unwrap()
                .build()
                .unwrap();

        let decryption_properties = FileDecryptionProperties::builder(footer_key.clone())
            .with_column_keys(column_names.clone(), column_keys.clone())
            .unwrap()
            .build()
            .unwrap();

        // Test round-trip
        let config_encrypt =
            ConfigFileEncryptionProperties::from(&file_encryption_properties);
        let encryption_properties_built =
            Arc::new(FileEncryptionProperties::from(config_encrypt.clone()));
        assert_eq!(file_encryption_properties, encryption_properties_built);

        let config_decrypt = ConfigFileDecryptionProperties::from(&decryption_properties);
        let decryption_properties_built =
            Arc::new(FileDecryptionProperties::from(config_decrypt.clone()));
        assert_eq!(decryption_properties, decryption_properties_built);

        ///////////////////////////////////////////////////////////////////////////////////
        // Test encryption config

        // Display original encryption config
        // println!("{:#?}", config_encrypt);

        let mut table_config = TableOptions::new();
        table_config.set_config_format(ConfigFileType::PARQUET);
        table_config
            .parquet
            .set(
                "crypto.file_encryption.encrypt_footer",
                config_encrypt.encrypt_footer.to_string().as_str(),
            )
            .unwrap();
        table_config
            .parquet
            .set(
                "crypto.file_encryption.footer_key_as_hex",
                config_encrypt.footer_key_as_hex.as_str(),
            )
            .unwrap();

        for (i, col_name) in column_names.iter().enumerate() {
            let key = format!("crypto.file_encryption.column_key_as_hex::{col_name}");
            let value = hex::encode(column_keys[i].clone());
            table_config
                .parquet
                .set(key.as_str(), value.as_str())
                .unwrap();
        }

        // Print matching final encryption config
        // println!("{:#?}", table_config.parquet.crypto.file_encryption);

        assert_eq!(
            table_config.parquet.crypto.file_encryption,
            Some(config_encrypt)
        );

        ///////////////////////////////////////////////////////////////////////////////////
        // Test decryption config

        // Display original decryption config
        // println!("{:#?}", config_decrypt);

        let mut table_config = TableOptions::new();
        table_config.set_config_format(ConfigFileType::PARQUET);
        table_config
            .parquet
            .set(
                "crypto.file_decryption.footer_key_as_hex",
                config_decrypt.footer_key_as_hex.as_str(),
            )
            .unwrap();

        for (i, col_name) in column_names.iter().enumerate() {
            let key = format!("crypto.file_decryption.column_key_as_hex::{col_name}");
            let value = hex::encode(column_keys[i].clone());
            table_config
                .parquet
                .set(key.as_str(), value.as_str())
                .unwrap();
        }

        // Print matching final decryption config
        // println!("{:#?}", table_config.parquet.crypto.file_decryption);

        assert_eq!(
            table_config.parquet.crypto.file_decryption,
            Some(config_decrypt.clone())
        );

        // Set config directly
        let mut table_config = TableOptions::new();
        table_config.set_config_format(ConfigFileType::PARQUET);
        table_config.parquet.crypto.file_decryption = Some(config_decrypt.clone());
        assert_eq!(
            table_config.parquet.crypto.file_decryption,
            Some(config_decrypt.clone())
        );
    }

    #[cfg(feature = "parquet_encryption")]
    #[test]
    fn parquet_encryption_factory_config() {
        let mut parquet_options = TableParquetOptions::default();

        assert_eq!(parquet_options.crypto.factory_id, None);
        assert_eq!(parquet_options.crypto.factory_options.options.len(), 0);

        let mut input_config = TestExtensionConfig::default();
        input_config
            .properties
            .insert("key1".to_string(), "value 1".to_string());
        input_config
            .properties
            .insert("key2".to_string(), "value 2".to_string());

        parquet_options
            .crypto
            .configure_factory("example_factory", &input_config);

        assert_eq!(
            parquet_options.crypto.factory_id,
            Some("example_factory".to_string())
        );
        let factory_options = &parquet_options.crypto.factory_options.options;
        assert_eq!(factory_options.len(), 2);
        assert_eq!(factory_options.get("key1"), Some(&"value 1".to_string()));
        assert_eq!(factory_options.get("key2"), Some(&"value 2".to_string()));
    }

    #[cfg(feature = "parquet")]
    #[test]
    fn parquet_table_options_config_entry() {
        let mut table_config = TableOptions::new();
        table_config.set_config_format(ConfigFileType::PARQUET);
        table_config
            .set("format.bloom_filter_enabled::col1", "true")
            .unwrap();
        let entries = table_config.entries();
        assert!(
            entries
                .iter()
                .any(|item| item.key == "format.bloom_filter_enabled::col1")
        )
    }

    #[cfg(feature = "parquet")]
    #[test]
    fn parquet_table_parquet_options_config_entry() {
        let mut table_parquet_options = TableParquetOptions::new();
        table_parquet_options
            .set(
                "crypto.file_encryption.column_key_as_hex::double_field",
                "31323334353637383930313233343530",
            )
            .unwrap();
        let entries = table_parquet_options.entries();
        assert!(
            entries.iter().any(|item| item.key
                == "crypto.file_encryption.column_key_as_hex::double_field")
        )
    }

    #[cfg(feature = "parquet")]
    #[test]
    fn parquet_table_options_config_metadata_entry() {
        let mut table_config = TableOptions::new();
        table_config.set_config_format(ConfigFileType::PARQUET);
        table_config.set("format.metadata::key1", "").unwrap();
        table_config.set("format.metadata::key2", "value2").unwrap();
        table_config
            .set("format.metadata::key3", "value with spaces ")
            .unwrap();
        table_config
            .set("format.metadata::key4", "value with special chars :: :")
            .unwrap();

        let parsed_metadata = table_config.parquet.key_value_metadata.clone();
        assert_eq!(parsed_metadata.get("should not exist1"), None);
        assert_eq!(parsed_metadata.get("key1"), Some(&Some("".into())));
        assert_eq!(parsed_metadata.get("key2"), Some(&Some("value2".into())));
        assert_eq!(
            parsed_metadata.get("key3"),
            Some(&Some("value with spaces ".into()))
        );
        assert_eq!(
            parsed_metadata.get("key4"),
            Some(&Some("value with special chars :: :".into()))
        );

        // duplicate keys are overwritten
        table_config.set("format.metadata::key_dupe", "A").unwrap();
        table_config.set("format.metadata::key_dupe", "B").unwrap();
        let parsed_metadata = table_config.parquet.key_value_metadata;
        assert_eq!(parsed_metadata.get("key_dupe"), Some(&Some("B".into())));
    }
}
